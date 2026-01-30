#include "azure/client_configuration/client_configuration.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

#include <thread>
#include <algorithm>

namespace runai::llm::streamer::impl::azure
{

ClientConfiguration::ClientConfiguration()
{
#ifdef AZURITE_TESTING
    // Connection string is only available for Azurite/local testing
    // Reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string
    const auto conn_str = utils::getenv<std::string>("AZURE_STORAGE_CONNECTION_STRING", "");
    if (!conn_str.empty()) {
        LOG(DEBUG) << "Using AZURE_STORAGE_CONNECTION_STRING for authentication (Azurite testing)";
        connection_string = conn_str;
    }
#endif

    // Account name configuration from environment variable
    // Authentication uses DefaultAzureCredential which supports:
    // - Environment variables (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
    // - Managed Identity
    // - Azure CLI
    // - Visual Studio Code
    // Reference: https://learn.microsoft.com/en-us/azure/developer/cpp/sdk/authentication
    const auto acct_name = utils::getenv<std::string>("AZURE_STORAGE_ACCOUNT_NAME", "");
    if (!acct_name.empty()) {
        LOG(DEBUG) << "Azure Storage account name: " << acct_name;
        account_name = acct_name;
    }

    const auto endpoint = utils::getenv<std::string>("AZURE_STORAGE_ENDPOINT", "");
    if (!endpoint.empty()) {
        LOG(DEBUG) << "Using custom Azure Storage endpoint: " << endpoint;
        endpoint_url = endpoint;
    }

    unsigned nprocs = std::thread::hardware_concurrency();
    LOG(SPAM) << "Hardware concurrency detected: " << nprocs;
    unsigned default_max_concurrency = nprocs == 0 ? 8U : 1U;
    unsigned worker_concurrency = utils::getenv<unsigned long>("RUNAI_STREAMER_CONCURRENCY", 8UL);
    LOG(SPAM) << "Streamer worker concurrency: " << worker_concurrency;
    max_concurrency = std::max(default_max_concurrency, nprocs * 2 / worker_concurrency);
    LOG(DEBUG) << "Azure Blob Storage per-client concurrency is set to: " << max_concurrency;

    // Note: Using Azure SDK defaults for timeouts and retries
    // Reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-retry-policy
}

}; // namespace runai::llm::streamer::impl::azure
