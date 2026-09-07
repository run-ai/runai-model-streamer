#include "azure/client_configuration/client_configuration.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

#include <cstdlib>
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

    // Account key for StorageSharedKeyCredential authentication
    const auto acct_key = utils::getenv<std::string>("AZURE_STORAGE_ACCOUNT_KEY", "");
    if (!acct_key.empty()) {
        LOG(DEBUG) << "Using AZURE_STORAGE_ACCOUNT_KEY for authentication";
        account_key = acct_key;
    }

    // SAS token for Shared Access Signature authentication
    const auto sas = utils::getenv<std::string>("AZURE_STORAGE_SAS_TOKEN", "");
    if (!sas.empty()) {
        LOG(DEBUG) << "Using AZURE_STORAGE_SAS_TOKEN for authentication";
        sas_token = sas;
    }

    // Endpoint suffix for sovereign clouds (default: blob.core.windows.net)
    const auto suffix = utils::getenv<std::string>("AZURE_STORAGE_ENDPOINT_SUFFIX", "");
    if (!suffix.empty()) {
        LOG(DEBUG) << "Using custom endpoint suffix: " << suffix;
        endpoint_suffix = suffix;
    }

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

    unsigned nprocs = std::thread::hardware_concurrency();
    LOG(SPAM) << "Hardware concurrency detected: " << nprocs;
    unsigned default_max_concurrency = nprocs == 0 ? 8U : 1U;
    // The number of clients the streamer will build, which cancels out below: N clients with
    // nprocs*2/N threads each. Reading the wrong variable over-provisions by exactly that factor.
    //
    // Both of these are DIVISORS below, and neither had any floor - a plain 0 in either variable
    // was an integer division by zero, no 2^32 required (env.h).
    const char * const variable = std::getenv("RUNAI_STREAMER_OBJ_CONCURRENCY") != nullptr
                                ? "RUNAI_STREAMER_OBJ_CONCURRENCY"
                                : "RUNAI_STREAMER_CONCURRENCY";
    unsigned worker_concurrency = utils::getenv_positive<unsigned>(variable, 8U);
    LOG(SPAM) << "Streamer worker concurrency: " << worker_concurrency;
    unsigned process_group_size = utils::getenv_positive<unsigned>("RUNAI_STREAMER_PROCESS_GROUP_SIZE", 1U);
    LOG(SPAM) << "Process group size: " << process_group_size;
    max_concurrency = std::max(default_max_concurrency, nprocs * 2 / (worker_concurrency * process_group_size));
    LOG(DEBUG) << "Azure Blob Storage per-client concurrency is set to: " << max_concurrency;

    // Note: Using Azure SDK defaults for timeouts and retries
    // Reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-retry-policy
}

}; // namespace runai::llm::streamer::impl::azure
