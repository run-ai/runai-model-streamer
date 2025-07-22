#include "common/s3_credentials/s3_credentials.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::s3
{

const char * const Credentials::ACCESS_KEY_ID_KEY = "access_key_id";
const char * const Credentials::SECRET_ACCESS_KEY_KEY = "secret_access_key";
const char * const Credentials::SESSION_TOKEN_KEY = "session_token";
const char * const Credentials::REGION_KEY = "region";

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

void Credentials::to_object_client_config(std::vector<common::backend_api::ObjectConfigParam_t> & config) const
{
    if (access_key_id.has_value())
    {
        config.push_back({ ACCESS_KEY_ID_KEY, access_key_id.value().c_str() });
    }

    if (secret_access_key.has_value())
    {
        config.push_back({ SECRET_ACCESS_KEY_KEY, secret_access_key.value().c_str() });
    }

    if (session_token.has_value())
    {
        config.push_back({ SESSION_TOKEN_KEY, session_token.value().c_str() });
    }

    if (region.has_value())
    {
        config.push_back({ REGION_KEY, region.value().c_str() });
    }
}

}; //namespace runai::llm::streamer::common::s3
