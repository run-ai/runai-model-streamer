#pragma once

#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::common::s3
{

struct Path
{
    Path() = default;
    Path(const StorageUri_C & path, unsigned index);

    // File bucket and path
    common::s3::StorageUri_C uri;

    // Index of file in the original files list
    unsigned index;
};

}; //namespace runai::llm::streamer::common::s3
