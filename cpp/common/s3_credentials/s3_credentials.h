#pragma once

#include <memory>
#include <string>

namespace runai::llm::streamer::common::s3
{

struct Credentials
{
    Credentials();

    Credentials(const char * access_key_id, const char * secret_access_key, const char * session_token);
    Credentials(const std::string & access_key_id, const std::string & secret_access_key, const std::string & session_token);

    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
};

}; //namespace runai::llm::streamer::common::s3
