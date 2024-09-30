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

    bool override_endpoint = utils::try_getenv("RUNAI_STREAMER_S3_ENDPOINT", endpoint);
    LOG_IF(DEBUG, override_endpoint) << "override url endpoint: " << endpoint;

    bucket = match[1];
    path = match[2];

    LOG(SPAM) << "endpoint: " << (endpoint.empty() ? "aws" : endpoint) <<  " bucket: " << bucket << " path: " << path;
}

std::ostream & operator<<(std::ostream & os, const StorageUri & uri)
{
    return os << "endpoint: " << (uri.endpoint.empty() ? "aws" : uri.endpoint) <<  " bucket: " << uri.bucket << " path: " << uri.path;
}

}; // namespace runai::llm::streamer::common::s3
