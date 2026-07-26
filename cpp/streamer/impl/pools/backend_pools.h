#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

#include "common/response_code/response_code.h"
#include "utils/threadpool/threadpool.h"
#include "streamer/impl/workload/workload.h"

namespace runai::llm::streamer::impl
{

// BackendPools - the streamer's worker threadpools, one per backend kind, each created lazily on first
// use. Filesystem reads are synchronous and want `concurrency` worker threads; object-storage reads are
// asynchronous (each worker owns a plugin client + in-flight capacity window) and want `s3_concurrency`.
// A streamer that only touches one backend spins up only one pool.
//
// Object storage is served by a single plugin (the backend handle is a process-wide static), so the one
// ObjectStorage pool serves one plugin. That small lock lives here too (an optional + leaf mutex).
//
// Keep this object in the streamer member slot the single ThreadPool used to occupy, so object-storage
// workers still join between S3Stop and S3Cleanup on teardown.
class BackendPools
{
 public:
    enum class Kind { FileSystem, ObjectStorage };
    enum class Plugin { S3, GCS, Azure };
    using Handler = utils::ThreadPool<Workload>::Handler;
    using WorkerFactory = utils::ThreadPool<Workload>::WorkerFactory;

    // filesystem_handler: the stateless synchronous handler for the filesystem pool.
    // object_storage_factory: builds a per-worker ObjectStorageWorker for the object-storage pool (async,
    // each worker owns its in-flight window).
    BackendPools(Handler filesystem_handler, WorkerFactory object_storage_factory, unsigned filesystem_size, unsigned object_storage_size);

    // Hand the workload to its kind's pool. The FileSystem pool is created lazily here on first use.
    // The ObjectStorage pool is NOT created here - it is created by lock_object_plugin, which async_request
    // always calls before dispatching object-storage workloads; pushing an object-storage workload before
    // then is a programming error (asserted). Thread-safe.
    void push(Kind kind, Workload && workload);

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

 private:
    Handler _filesystem_handler;
    WorkerFactory _object_storage_factory;
    unsigned _filesystem_size;
    unsigned _object_storage_size;

    std::once_flag _filesystem_once;
    std::unique_ptr<utils::ThreadPool<Workload>> _filesystem_pool;

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
