#include "streamer/impl/pools/backend_pools.h"

#include <atomic>
#include <utility>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

BackendPools::BackendPools(Handler filesystem_handler,
                           WorkerFactory filesystem_async_factory,
                           WorkerFactory object_storage_factory,
                           unsigned filesystem_size,
                           unsigned object_storage_size) :
    _filesystem_handler(std::move(filesystem_handler)),
    _filesystem_async_factory(std::move(filesystem_async_factory)),
    _object_storage_factory(std::move(object_storage_factory)),
    _filesystem_size(filesystem_size),
    _object_storage_size(object_storage_size)
{}

void BackendPools::push(Pool pool, Workload && workload)
{
    if (pool == Pool::ObjectStorage)
    {
        // Created by lock_object_plugin (once the plugin is known), which async_request always calls
        // before dispatching object-storage workloads - so no per-workload creation check on this path.
        ASSERT(_object_storage_pool != nullptr) << "object-storage workload dispatched before its plugin was locked";
        _object_storage_pool->push(std::move(workload));
        return;
    }

    if (pool == Pool::FileSystemAsync)
    {
        // ONE thread: the worker owns an IoEngine, which is not thread safe, and ThreadPool's workers
        // pull from a shared deque - so a second thread here would be a second engine serving the
        // same queue, with no way to keep a workload on one of them.
        //
        // Lazy like the synchronous pool and for the same reason: a streamer that never resolves to
        // an async strategy never builds a ring or a thread. Nothing needs locking first, unlike
        // object storage, because there is no plugin to agree on - the strategy was already settled
        // by StrategyResolver before dispatch.
        std::call_once(_filesystem_async_once, [this]()
        {
            _filesystem_async_pool = std::make_unique<utils::ThreadPool<Workload>>(_filesystem_async_factory, 1);
        });
        _filesystem_async_pool->push(std::move(workload));
        return;
    }

    // filesystem has no plugin to wait for, so create its pool lazily on first use
    std::call_once(_filesystem_once, [this]()
    {
        _filesystem_pool = std::make_unique<utils::ThreadPool<Workload>>(_filesystem_handler, _filesystem_size);
    });
    _filesystem_pool->push(std::move(workload));
}

common::ResponseCode BackendPools::lock_object_plugin(Plugin plugin)
{
    // Fast path (no mutex): once the lock is fully established - plugin recorded AND pool created, published
    // together via _ready_plugin - a matching submission needs nothing more. The acquire pairs with the
    // release below, so observing _ready_plugin here guarantees the pool exists and is safe to dispatch to.
    // (Credentials are streamer-scoped, set once via runai_set_credentials - not per submission - so there is
    // nothing credential-related to lock here.)
    const int ready = _ready_plugin.load(std::memory_order_acquire);
    if (ready != -1)
    {
        if (ready != static_cast<int>(plugin))
        {
            LOG(ERROR) << "Streamer is locked to a single object storage plugin; rejecting a submission using a different plugin";
            return common::ResponseCode::UnsupportedBackendMix;
        }
        return common::ResponseCode::Success;
    }

    // Slow path: taken only until the lock is initialized (the first object-storage submission, plus any
    // concurrent submitters racing with it).
    const auto guard = std::unique_lock<std::mutex>(_plugin_mutex);

    if (!_plugin.has_value())
    {
        // First object-storage submission: create the pool, THEN commit the lock. Order matters for exception
        // safety - the ThreadPool ctor spawns worker threads and can throw (e.g. thread creation fails under a
        // process/fd/thread ulimit, not only OOM). Building it first means a throw leaves _plugin unset and
        // _ready_plugin at -1 with the pool still null, so the NEXT submission retries this branch cleanly -
        // instead of finding the plugin "locked" with a null pool, which would ASSERT on _pools.push.
        // per-worker pool: each thread owns an ObjectStorageWorker (from the factory) with its own in-flight window
        _object_storage_pool = std::make_unique<utils::ThreadPool<Workload>>(_object_storage_factory, _object_storage_size);
        // Commit the lock only now that the pool exists. _ready_plugin is stored last (release), so a fast-path
        // acquire that observes it is guaranteed the pool is built and safe to dispatch to. The s3_wrapper
        // backend handle is a process-wide static, hence the single-plugin lock.
        _plugin = plugin;
        _ready_plugin.store(static_cast<int>(plugin), std::memory_order_release);
        return common::ResponseCode::Success;
    }

    // Already locked by a concurrent submitter between the fast-path load and here: verify the plugin.
    if (_plugin.value() != plugin)
    {
        LOG(ERROR) << "Streamer is locked to a single object storage plugin; rejecting a submission using a different plugin";
        return common::ResponseCode::UnsupportedBackendMix;
    }

    return common::ResponseCode::Success;
}

bool BackendPools::async_pool_used() const
{
    return _filesystem_async_pool != nullptr;
}

unsigned BackendPools::pools_created() const
{
    return (_filesystem_pool != nullptr ? 1u : 0u)
         + (_filesystem_async_pool != nullptr ? 1u : 0u)
         + (_object_storage_pool != nullptr ? 1u : 0u);
}

}; // namespace runai::llm::streamer::impl
