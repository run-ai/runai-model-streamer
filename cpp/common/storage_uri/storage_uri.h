#pragma once

#include <string>

namespace runai::llm::streamer::common::s3
{

struct StorageUri
{
    StorageUri(const std::string & uri);

    std::string bucket;
    std::string path;
    std::string endpoint;
};

struct StorageUri_C
{
    StorageUri_C(const StorageUri & uri);

    const char * bucket;
    const char * path;
    const char * endpoint;
};

std::ostream & operator<<(std::ostream &, const StorageUri &);

}; //namespace runai::llm::streamer::common::s3
