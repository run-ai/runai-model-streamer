#include "gcs/client_configuration/client_configuration.h"

#include "google/cloud/storage/client.h"

#include "common/exception/exception.h"
#include "common/response_code/response_code.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

#include <fstream>

namespace runai::llm::streamer::impl::gcs
{

ClientConfiguration::ClientConfiguration()
{
    const auto max_connections = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_MAX_CONNECTIONS", 0);
    if (max_connections)
    {
        options.set<google::cloud::storage::ConnectionPoolSizeOption>(max_connections);
    }

    // if the transfer speed is less than the low speed limit for request_timeout_ms milliseconds the transfer is aborted and retried
    const auto request_timeout_ms = utils::getenv<unsigned long>("RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS", 600000);
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

    const auto sa_key_file_name = utils::getenv<std::string>("RUNAI_STREAMER_GCS_CREDENTIAL_FILE", "");
    if (!sa_key_file_name.empty()) {
        LOG(DEBUG) << "Loading credentials for Service Account from file: " << sa_key_file_name;
        try {
            // Open the file stream.
            auto is = std::ifstream(sa_key_file_name);

            // Configure the stream to throw an exception on any failure (e.g., file not found, read error).
            is.exceptions(std::ifstream::failbit | std::ifstream::badbit);

            // Read the entire file into the 'contents' string.
            auto contents = std::string(std::istreambuf_iterator<char>(is.rdbuf()), {});

            // Use the contents to set the credentials.
            options.set<google::cloud::UnifiedCredentialsOption>(
                google::cloud::MakeServiceAccountCredentials(contents));
        } catch (const std::ios_base::failure& ex) {
            LOG(ERROR) << "Failed to read service account key file: " << sa_key_file_name;
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
    }
}

}; // namespace runai::llm::streamer::impl::gcs
