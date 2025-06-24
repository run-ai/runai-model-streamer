#include <regex>
#include <vector>

#include "common/storage_uri/storage_uri.h"

#include "utils/logging/logging.h"

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

    bucket = match[1];
    path = match[2];

    LOG(SPAM) << "bucket: " << bucket << " path: " << path;
}

std::ostream & operator<<(std::ostream & os, const StorageUri & uri)
{
    return os << "bucket: " << uri.bucket << " path: " << uri.path;
}

StorageUri_C::StorageUri_C(const StorageUri & uri) :
    bucket(uri.bucket.c_str()),
    path(uri.path.c_str())
{}

}; // namespace runai::llm::streamer::common::s3
