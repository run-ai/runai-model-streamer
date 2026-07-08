#include "s3/client_configuration/client_configuration.h"

#include <algorithm>

#include "common/backend_api/object_storage/object_storage.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::s3
{

size_t inflight_window_bytes(size_t chunk_bytesize, double target_gbps)
{
    // explicit override, in MiB
    const size_t mib = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_MAX_INFLIGHT_MIB", 0);
    if (mib != 0)
    {
        const size_t result = mib * 1024 * 1024;
        LOG(INFO) << "S3 in-flight window: " << utils::logging::human_readable_size(result) << " (RUNAI_STREAMER_S3_MAX_INFLIGHT_MIB=" << mib << ")";
        return result;
    }

    // otherwise derive a bandwidth-delay product from the target throughput, floored at a
    // couple of read chunks so the window can always keep the pipe busy. chunk_bytesize is
    // the client's configured default_storage_chunk_size; target_gbps is throughputTargetGbps.
    const double bytes_per_sec = target_gbps * 1e9 / 8.0;   // Gbps (gigabits) -> bytes/s
    const double nominal_latency_s = 0.1;                   // ~100 ms
    const double margin = common::backend_api::inflight_window_margin();
    const size_t window = static_cast<size_t>(bytes_per_sec * nominal_latency_s * margin);

    const size_t result = std::max(window, 2 * chunk_bytesize);
    LOG(DEBUG) << "S3 in-flight window: " << utils::logging::human_readable_size(result)
              << " (target throughput " << target_gbps << " Gbps, chunk " << utils::logging::human_readable_size(chunk_bytesize) << ")";
    return result;
}

ClientConfiguration::ClientConfiguration()
{
    unsigned long max_connections = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_MAX_CONNECTIONS", 0);
    if (max_connections)
    {
        config.maxConnections = max_connections;
    }

    unsigned long target_gbps = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_TARGET_GBPS", 0);
    if (target_gbps)
    {
        LOG(DEBUG) << "S3 target throughput is set to " << target_gbps << " Gbps";
        config.throughputTargetGbps = target_gbps;
    }

    // if the transfer speed is less than the low speed limit for request_timeout_ms milliseconds the transfer is aborted and retried
    const auto request_timeout_ms = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS", 1000);
    if (request_timeout_ms)
    {
        LOG(DEBUG) << "S3 request timeout is set to " << request_timeout_ms << " ms";
        config.requestTimeoutMs = request_timeout_ms;
    }

    // aws sdk default is 1 byte/second
    const auto low_speed_limit = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_LOW_SPEED_LIMIT", 0);
    if (low_speed_limit)
    {
        LOG(DEBUG) << "S3 minimum speed is set to " << low_speed_limit << " bytes in second";
        config.lowSpeedLimit = low_speed_limit;
    }
}

}; // namespace runai::llm::streamer::impl::s3
