#pragma once

#include <memory>
#include <string>
#include <optional>
#include <vector>

#include "common/backend_api/object_storage/object_storage.h"

namespace runai::llm::streamer::common::s3
{

struct Credentials
{
    Credentials();

    Credentials(const char * access_key_id, const char * secret_access_key, const char * session_token, const char * region, const char * endpoint);

    void to_object_client_config(std::vector<common::backend_api::ObjectConfigParam_t> & config) const;

    std::optional<std::string> access_key_id;
    std::optional<std::string> secret_access_key;
    std::optional<std::string> session_token;
    std::optional<std::string> region;
    std::optional<std::string> endpoint;

    static const char * const ACCESS_KEY_ID_KEY;
    static const char * const SECRET_ACCESS_KEY_KEY;
    static const char * const SESSION_TOKEN_KEY;
    static const char * const REGION_KEY;
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

}; //namespace runai::llm::streamer::common::obj_store
