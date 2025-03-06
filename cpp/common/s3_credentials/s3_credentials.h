#pragma once

#include <memory>
#include <string>
#include <optional>

namespace runai::llm::streamer::common::s3
{

struct Credentials
{
    Credentials();

    Credentials(const char * access_key_id, const char * secret_access_key, const char * session_token, const char * region, const char * endpoint);

    std::optional<std::string> access_key_id;
    std::optional<std::string> secret_access_key;
    std::optional<std::string> session_token;
    std::optional<std::string> region;
    std::optional<std::string> endpoint;
};

struct Credentials_C
{
    Credentials_C(const Credentials & credentials);

    const char * access_key_id;
    const char * secret_access_key;
    const char * session_token;
    const char * region;
    const char * endpoint;
};

}; //namespace runai::llm::streamer::common::s3
