#pragma once

#include <string>
#include <optional>

// Note: In a real implementation, you would include Azure SDK headers:
// #include <azure/storage/blobs.hpp>

namespace runai::llm::streamer::impl::azure
{

struct ClientConfiguration
{
    // Azure client configuration options
    std::optional<std::string> connection_string;
    std::optional<std::string> account_name;
    std::optional<std::string> account_key;
    std::optional<std::string> sas_token;
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
