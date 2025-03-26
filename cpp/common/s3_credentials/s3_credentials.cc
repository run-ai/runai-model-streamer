#include "common/s3_credentials/s3_credentials.h"

namespace runai::llm::streamer::common::s3
{

Credentials::Credentials(const char * access_key_id, const char * secret_access_key, const char * session_token, const char * region, const char * endpoint) :
    access_key_id(access_key_id ? std::optional<std::string>(access_key_id) : std::nullopt),
    secret_access_key(secret_access_key ? std::optional<std::string>(secret_access_key) : std::nullopt),
    session_token(session_token ? std::optional<std::string>(session_token) : std::nullopt),
    region(region ? std::optional<std::string>(region) : std::nullopt),
    endpoint(endpoint ? std::optional<std::string>(endpoint) : std::nullopt)
{}

Credentials::Credentials() : Credentials(nullptr, nullptr, nullptr, nullptr, nullptr)
{}

Credentials_C::Credentials_C(const Credentials & credentials) :
    access_key_id(credentials.access_key_id.has_value() ? credentials.access_key_id.value().c_str() : nullptr),
    secret_access_key(credentials.secret_access_key.has_value() ? credentials.secret_access_key.value().c_str() : nullptr),
    session_token(credentials.session_token.has_value() ? credentials.session_token.value().c_str() : nullptr),
    region(credentials.region.has_value() ? credentials.region.value().c_str() : nullptr),
    endpoint(credentials.endpoint.has_value() ? credentials.endpoint.value().c_str() : nullptr)
{}

}; //namespace runai::llm::streamer::common::s3
