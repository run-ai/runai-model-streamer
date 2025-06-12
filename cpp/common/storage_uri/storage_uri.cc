#include <regex>
#include <vector>

#include "common/storage_uri/storage_uri.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::common::s3
{

StorageUri::StorageUri(const std::string & uri)
{
    static const std::regex awsRegex("^s3://([^/]+)/(.+)$");

    std::smatch match;

    LOG(SPAM) << "Checking: " << uri;

    if (!std::regex_match(uri, match, awsRegex))
    {
        LOG(SPAM) << "'" << uri << "' is not in s3 format";
        throw std::exception();
    }

    bool override_endpoint = utils::try_getenv("AWS_ENDPOINT_URL", endpoint);
    bool override_endpoint_flag = utils::getenv<bool>("RUNAI_STREAMER_OVERRIDE_ENDPOINT_URL", true);
    LOG_IF(DEBUG, override_endpoint) << "override url endpoint: " << endpoint;
    LOG_IF(DEBUG, override_endpoint_flag && override_endpoint) << "direct override of url endpoint in client configuration";

    bucket = match[1];
    path = match[2];

    LOG(SPAM) << "endpoint: " << (endpoint.empty() ? "aws" : endpoint) <<  " bucket: " << bucket << " path: " << path;
}

std::ostream & operator<<(std::ostream & os, const StorageUri & uri)
{
    return os << "endpoint: " << (uri.endpoint.empty() ? "aws" : uri.endpoint) <<  " bucket: " << uri.bucket << " path: " << uri.path;
}

StorageUri_C::StorageUri_C(const StorageUri & uri) :
    bucket(uri.bucket.c_str()),
    path(uri.path.c_str()),
    endpoint(uri.endpoint.empty() ? nullptr : uri.endpoint.c_str())
{}

}; // namespace runai::llm::streamer::common::s3
