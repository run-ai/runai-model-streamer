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
    // Connection string is only available for Azurite/local testing
    std::optional<std::string> connection_string;
#endif
    std::optional<std::string> endpoint_url;
    
    // Concurrency settings
    unsigned int max_concurrency = 8;
    
    ClientConfiguration();
};

} // namespace runai::llm::streamer::impl::azure
