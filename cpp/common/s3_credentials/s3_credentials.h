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

    // Value equality over all fields (an unset field only equals an unset field). Used to lock a
    // streamer to a single set of object-storage credentials and reject a differing submission.
    bool operator==(const Credentials & other) const;
    bool operator!=(const Credentials & other) const;

    // True when no field is set: no credentials were passed, so the ambient/default provider chain is
    // used. Such a submission is always credential-compatible (it neither locks nor is checked).
    bool empty() const;

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

}; //namespace runai::llm::streamer::common::s3
