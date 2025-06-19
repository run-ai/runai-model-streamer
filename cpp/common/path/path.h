#pragma once

#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::common::s3
{

struct Path
{
    Path() = default;
    Path(const StorageUri_C & path);

    // File bucket and path
    common::s3::StorageUri_C uri;
};

}; //namespace runai::llm::streamer::common::s3
