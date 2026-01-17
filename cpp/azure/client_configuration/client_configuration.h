#pragma once

#include <string>
#include <optional>

namespace runai::llm::streamer::impl::azure
{

struct ClientConfiguration
{
    // Azure client configuration options (uses DefaultAzureCredential)
    std::optional<std::string> account_name;
#ifdef AZURITE_TESTING
    std::optional<std::string> account_key;  // For Azurite/local testing only (requires -DAZURITE_TESTING)
#endif
    std::optional<std::string> endpoint_url;
    
    // Retry and timeout settings
    unsigned int max_retries = 3;
    unsigned int retry_delay_ms = 1000;
    unsigned int request_timeout_s = 300;
    
    // Concurrency settings
    unsigned int max_concurrency = 8;
    
    ClientConfiguration();
};

} // namespace runai::llm::streamer::impl::azure
