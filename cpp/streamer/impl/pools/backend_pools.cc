#include "streamer/impl/pools/backend_pools.h"

#include <utility>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

BackendPools::BackendPools(Handler filesystem_handler, WorkerFactory object_storage_factory, unsigned filesystem_size, unsigned object_storage_size) :
    _filesystem_handler(std::move(filesystem_handler)),
    _object_storage_factory(std::move(object_storage_factory)),
    _filesystem_size(filesystem_size),
    _object_storage_size(object_storage_size)
{}

void BackendPools::push(Kind kind, Workload && workload)
{
    if (kind == Kind::ObjectStorage)
    {
        // Created by lock_object_plugin (once the plugin is known), which async_request always calls
        // before dispatching object-storage workloads - so no per-workload creation check on this path.
        ASSERT(_object_storage_pool != nullptr) << "object-storage workload dispatched before its plugin was locked";
        _object_storage_pool->push(std::move(workload));
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
    {
        // first object-storage submission records the plugin; a later differing plugin is rejected
        const auto guard = std::unique_lock<std::mutex>(_plugin_mutex);
        if (!_plugin.has_value())
        {
            _plugin = plugin;
        }
        else if (_plugin.value() != plugin)
        {
            LOG(ERROR) << "Streamer is locked to a single object storage plugin; rejecting a submission using a different plugin";
            return common::ResponseCode::UnsupportedBackendMix;   // build nothing
        }
    }

    // plugin locked: create the ObjectStorage pool once. std::call_once is thread-safe for concurrent
    // submitters and creates it exactly once.
    std::call_once(_object_storage_once, [this]()
    {
        // per-worker pool: each thread owns an ObjectStorageWorker (from the factory) with its own in-flight window
        _object_storage_pool = std::make_unique<utils::ThreadPool<Workload>>(_object_storage_factory, _object_storage_size);
    });

    return common::ResponseCode::Success;
}

unsigned BackendPools::pools_created() const
{
    return (_filesystem_pool != nullptr ? 1u : 0u) + (_object_storage_pool != nullptr ? 1u : 0u);
}

}; // namespace runai::llm::streamer::impl
