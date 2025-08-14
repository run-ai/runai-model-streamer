#include "s3/client_configuration/client_configuration.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::s3
{

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
