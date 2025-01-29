#pragma once

#include <string>

namespace runai::llm::streamer::common::s3
{

struct StorageUri
{
    enum class Type
    {
        Object,
        Path,
    };

    StorageUri(const std::string & uri, Type type = Type::Object);

    std::string bucket;
    std::string path;
    std::string endpoint;
};

std::ostream & operator<<(std::ostream &, const StorageUri &);

}; //namespace runai::llm::streamer::common::s3
