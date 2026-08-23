#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <map>
#include <optional>

#include <sys/types.h>

#include "common/response_code/response_code.h"
#include "utils/threadpool/threadpool.h"
#include "streamer/impl/workload/workload.h"

namespace runai::llm::streamer::impl
{

// BackendPools - the streamer's worker threadpools, one per destination, each created lazily on first
// use. A streamer that only touches one destination spins up only one pool.
//
//   FileSystem       synchronous pread on `concurrency` threads. EXACTLY ONE POOL, always - the
//                    synchronous reader is not modified by the async work, and it has no shared
//                    submission point to wedge, so there is nothing to isolate by splitting it.
//   FileSystemAsync  ONE POOL PER MOUNT. Each pool has one thread, which owns an AsyncIoWorker and
//                    its IoEngine. One thread per pool, because the engine is not thread safe (5.2).
//                    A separate pool per mount, not one pool with several workers: all workers in a
//                    pool take work from the same queue, so one pool could not keep a mount's work
//                    on one engine.
//   ObjectStorage    `s3_concurrency` threads, each worker owning a plugin client and its own
//                    in-flight window.
//
// Object storage is served by a single plugin (the backend handle is a process-wide static), so the one
// ObjectStorage pool serves one plugin. That small lock lives here too (an optional + leaf mutex).
//
// Keep this object in the streamer member slot the single ThreadPool used to occupy, so object-storage
// workers still join between S3Stop and S3Cleanup on teardown.
class BackendPools
{
 public:
    // Where a workload goes. Three values rather than a {destination, strategy} pair: a pair can
    // express {ObjectStorage, IoUringDirect}, which is nonsense rejectable only at runtime - the same
    // defect the flat Strategy enum avoids.
    //
    // Strategy is NOT this. It stays the configured preference and the per-file result;
    // io_uring_* and libaio_* select FileSystemAsync, sync_buffered selects FileSystem.
    enum class Pool { FileSystem, FileSystemAsync, ObjectStorage };
    enum class Plugin { S3, GCS, Azure };
    using Handler = utils::ThreadPool<Workload>::Handler;
    using WorkerFactory = utils::ThreadPool<Workload>::WorkerFactory;

    // filesystem_handler: the stateless synchronous handler for the filesystem pool.
    // object_storage_factory: builds a per-worker ObjectStorageWorker for the object-storage pool (async,
    // each worker owns its in-flight window).
    // filesystem_async_factory builds the AsyncIoWorker; its pool is always one thread, so no size is
    // taken for it.
    BackendPools(Handler filesystem_handler,
                 WorkerFactory filesystem_async_factory,
                 WorkerFactory object_storage_factory,
                 unsigned filesystem_size,
                 unsigned object_storage_size);

    // Hand the workload to its pool. FileSystem and ObjectStorage only - Pool::FileSystemAsync is
    // REJECTED here (asserted), because an async workload is routed by its mount and there is no
    // device to route by in this signature. Use push_async.
    //
    // The FileSystem pool is created lazily here on first use. The ObjectStorage pool is NOT created
    // here - it is created by lock_object_plugin, which async_request always calls before dispatching
    // object-storage workloads; pushing an object-storage workload before then is a programming error
    // (asserted). Thread-safe.
    void push(Pool pool, Workload && workload);

    // Hand an async filesystem workload to the engine for its mount, creating that engine on first
    // use. `device` is the mount's st_dev.
    //
    // Above RUNAI_STREAMER_FS_MAX_ENGINES, mounts SHARE the engine that has the least work waiting.
    // They do not wait for a free engine. Waiting would only move the delay to another place.
    //
    // Sharing is logged as a warning, once per mount, and the log names the variable. Without that
    // warning the cost is invisible: mounts are separated only up to the limit. Above it, a mount
    // that stops responding also stops the mounts that share its engine.
    //
    // A mount keeps the SAME engine for as long as the streamer lives. The engine holds the state
    // that routes completions and counts free slots, so a mount cannot move while it still has reads
    // running. A stuck mount therefore keeps its engine forever. That is the separation working, not
    // a leak.
    void push_async(dev_t device, Workload && workload);

    // Lock object storage to a single plugin and create the ObjectStorage pool (once). The first
    // object-storage submission records the plugin and builds the pool; a later submission with a different
    // plugin returns UnsupportedBackendMix (and builds nothing). The lock enforces the single-plugin
    // constraint (the s3_wrapper backend handle is a process-wide static). Credentials are streamer-scoped
    // (set once via runai_set_credentials), not per submission, so nothing credential-related is locked here.
    // The ObjectStorageWorkers are plugin-agnostic (they dispatch by URI). Creating the pool here - always
    // called before dispatch - keeps it off the per-workload push path. Thread-safe.
    common::ResponseCode lock_object_plugin(Plugin plugin);

    // For testing: number of pools created so far.
    unsigned pools_created() const;

    // How many async engines exist. For tests: nothing in production reads it.
    unsigned async_engines() const;

    // How many mounts had to share an engine because the limit was reached. Zero when every mount got
    // its own. Counted here because only this class knows which mounts were refused an engine.
    unsigned shared_engine_mounts() const;

    // Whether the async pool exists. Pools are created lazily on first push, so non-null means at
    // least one workload was actually routed here - which is the only externally visible difference
    // between an io_uring read and a synchronous one, since both return identical bytes.
    bool async_pool_used() const;

 private:
    Handler _filesystem_handler;
    WorkerFactory _filesystem_async_factory;
    WorkerFactory _object_storage_factory;
    unsigned _filesystem_size;
    unsigned _object_storage_size;

    std::once_flag _filesystem_once;
    std::unique_ptr<utils::ThreadPool<Workload>> _filesystem_pool;

    // One pool per mount, keyed on st_dev. Each is created on first use and lives as long as the
    // streamer. The mutex guards the two maps below. Several threads can submit at the same time, so
    // the first workload for a mount can arrive on more than one thread at once.
    mutable std::mutex _async_mutex;
    std::map<dev_t, std::unique_ptr<utils::ThreadPool<Workload>>> _async_pools;

    // Which engine each mount uses. This is separate from _async_pools because above the limit
    // several mounts point at the same pool, and because a mount must keep the engine it was given.
    std::map<dev_t, utils::ThreadPool<Workload> *> _async_by_device;

    // The largest number of engines. 1 puts every mount on one engine. That is the default, and it
    // is also how an operator turns the feature off, because we have not measured the speed gain.
    const unsigned _max_async_engines;

    // Mounts that were given an engine already in use. Not the same as (mounts - engines): a mount
    // that arrives after the limit is reached is counted here even if it is the only one sharing.
    unsigned _shared_mounts = 0;

    // The engine with the least queued work. Called under _async_mutex.
    utils::ThreadPool<Workload> * least_loaded_async() const;

    std::unique_ptr<utils::ThreadPool<Workload>> _object_storage_pool;

    // The object-storage plugin lock. The first object-storage submission records the plugin and creates the
    // pool under _plugin_mutex; a later submission only compares. _plugin is written once (first submission)
    // and guarded by _plugin_mutex thereafter. Leaf mutex - guards concurrent submitters.
    std::mutex _plugin_mutex;
    std::optional<Plugin> _plugin;

    // Lock-free fast-path gate: -1 until the lock is fully established (plugin recorded AND pool created),
    // then the locked Plugin as int. Published with release AFTER the pool exists, so a fast-path acquire
    // that observes it is guaranteed the pool is usable. Lets a submission take the mutex only until the lock
    // is initialized (the first object-storage submission); after that every submission is served lock-free.
    std::atomic<int> _ready_plugin { -1 };
};

}; // namespace runai::llm::streamer::impl
