#pragma once

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

    // Lock object storage to a single plugin AND create the ObjectStorage pool (once). The first call records
    // the plugin and builds the pool; a later differing plugin returns UnsupportedBackendMix (and builds
    // nothing). The lock enforces the single-plugin constraint (the s3_wrapper backend handle is a
    // process-wide static); the ObjectStorageWorkers themselves are plugin-agnostic (they dispatch by URI).
    // Creating the pool here - always called before dispatch - keeps it off the per-workload push path.
    // Thread-safe.
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

    std::once_flag _object_storage_once;
    std::unique_ptr<utils::ThreadPool<Workload>> _object_storage_pool;

    // the single object-storage plugin the ObjectStorage pool serves (leaf mutex - guards concurrent submitters)
    std::mutex _plugin_mutex;
    std::optional<Plugin> _plugin;
};

}; // namespace runai::llm::streamer::impl
