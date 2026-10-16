/*
 * SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "azure/client_configuration/client_configuration.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

#include <thread>
#include <algorithm>
#include <charconv>
#include <cstring>
#include <stdexcept>

namespace runai::llm::streamer::impl::azure
{

namespace
{
unsigned process_group_size(const common::backend_api::ObjectClientConfig_t & config)
{
    for (unsigned i = 0; config.initial_params && i < config.num_initial_params; ++i)
    {
        const auto & param = config.initial_params[i];
        if (param.key && std::strcmp(param.key, "process_group_size") == 0)
        {
            unsigned count = 0;
            const char * end = param.value ? param.value + std::strlen(param.value) : "";
            const char * begin = param.value ? param.value : end;
            const auto parsed = std::from_chars(begin, end, count);
            if (parsed.ec != std::errc() || parsed.ptr != end || count == 0)
            {
                throw std::invalid_argument("process_group_size must be a positive unsigned integer");
            }
            return count;
        }
    }
    return 1;
}
} // namespace

ClientConfiguration::ClientConfiguration(const common::backend_api::ObjectClientConfig_t & config) :
    ClientConfiguration(config.concurrent_readers, process_group_size(config))
{}

ClientConfiguration::ClientConfiguration(unsigned concurrent_readers, unsigned process_group_size)
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
    // A DIVISOR below, so it must not be zero: an unstated count means the caller said nothing, not
    // that it will build no clients.
    // Floored, not defaulted: a zero would divide by zero, and only a caller outside the streamer can
    // send one.
    const unsigned worker_concurrency = std::max(1U, concurrent_readers);
    LOG(SPAM) << "Streamer worker concurrency: " << worker_concurrency;
    process_group_size = utils::getenv_positive<unsigned>("RUNAI_STREAMER_PROCESS_GROUP_SIZE", process_group_size);
    LOG(SPAM) << "Process group size: " << process_group_size;
    max_concurrency = std::max(default_max_concurrency, nprocs * 2 / worker_concurrency / process_group_size);
    LOG(DEBUG) << "Azure Blob Storage per-client concurrency is set to: " << max_concurrency;

    // Note: Using Azure SDK defaults for timeouts and retries
    // Reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-retry-policy
}

}; // namespace runai::llm::streamer::impl::azure
