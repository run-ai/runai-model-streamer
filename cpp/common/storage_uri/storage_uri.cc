#include <regex>
#include <vector>

#include "common/storage_uri/storage_uri.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::common::s3
{

StorageUri::StorageUri(const std::string & uri, StorageUri::Type type)
{
    static const std::regex aws_object_regex("^s3://([^/]+)/(.+)$");
    static const std::regex aws_path_regex("^s3://([^/]+)(?:/(.*))?$");

    std::smatch match;

    LOG(SPAM) << "Checking " << uri << " for " << (type == Type::Object ? "object" : "path");

    if (!std::regex_match(uri, match, (type == Type::Object ? aws_object_regex : aws_path_regex)))
    {
        LOG(SPAM) << "'" << uri << "' is not in s3 format";
        throw std::exception();
    }

    bool override_endpoint = utils::try_getenv("RUNAI_STREAMER_S3_ENDPOINT", endpoint);
    LOG_IF(DEBUG, override_endpoint) << "override url endpoint: " << endpoint;

    bucket = match[1];

    if (match[2].matched)
    {
        path = match[2];
    }

    LOG(SPAM) << "endpoint: " << (endpoint.empty() ? "aws" : endpoint) <<  " bucket: " << bucket << " path: " << path;
}

std::ostream & operator<<(std::ostream & os, const StorageUri & uri)
{
    return os << "endpoint: " << (uri.endpoint.empty() ? "aws" : uri.endpoint) <<  " bucket: " << uri.bucket << " path: " << uri.path;
}

}; // namespace runai::llm::streamer::common::s3
