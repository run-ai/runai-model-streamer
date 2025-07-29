#include "gcs/client_configuration/client_configuration.h"

#include "google/cloud/storage/client.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::gcs
{

ClientConfiguration::ClientConfiguration()
{
    unsigned max_connections = utils::getenv<bool>("RUNAI_STREAMER_S3_MAX_CONNECTIONS", 0);
    if (max_connections)
    {
        options.set<google::cloud::storage::ConnectionPoolSizeOption>(max_connections);
    }

    // if the transfer speed is less than the low speed limit for request_timeout_ms milliseconds the transfer is aborted and retried
    const auto request_timeout_ms = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS", 60000);
    if (request_timeout_ms)
    {
        LOG(DEBUG) << "GCS request timeout is set to " << request_timeout_ms << " ms";
        std::chrono::seconds request_timeout_seconds = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::milliseconds(request_timeout_ms));
        options.set<google::cloud::storage::DownloadStallTimeoutOption>(request_timeout_seconds);
    }

    const auto low_speed_limit = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_LOW_SPEED_LIMIT", 0);
    if (low_speed_limit)
    {
        LOG(DEBUG) << "GCS minimum speed is set to " << low_speed_limit << " bytes in second";
        options.set<google::cloud::storage::DownloadStallMinimumRateOption>(low_speed_limit);
    }

    const auto trace_gcs = utils::getenv<bool>("RUNAI_STREAMER_S3_TRACE", false);
    if (trace_gcs)
    {
        LOG(DEBUG) << "Enabling log tracing for rpc/auth/http modules for GCS API calls";
        std::set<std::string> logging_components;
        logging_components.insert("rpc");
        logging_components.insert("auth");
        logging_components.insert("http");

        options.set<google::cloud::LoggingComponentsOption>(std::move(logging_components));
    }
}

}; // namespace runai::llm::streamer::impl::gcs
