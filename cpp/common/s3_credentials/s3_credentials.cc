#include "common/s3_credentials/s3_credentials.h"

namespace runai::llm::streamer::common::s3
{

Credentials::Credentials(const char * access_key_id, const char * secret_access_key, const char * session_token) :
    access_key_id(access_key_id == nullptr ? "" : access_key_id),
    secret_access_key(secret_access_key == nullptr ? "" : secret_access_key),
    session_token(session_token == nullptr ? "" : session_token)
{}


Credentials::Credentials(const std::string & access_key_id, const std::string & secret_access_key, const std::string & session_token) :
    access_key_id(access_key_id),
    secret_access_key(secret_access_key),
    session_token(session_token)
{}

Credentials::Credentials() : Credentials("", "", "")
{}

}; //namespace runai::llm::streamer::common::s3
