#pragma once

#include "google/cloud/common_options.h"

#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::impl::gcs
{

struct ClientConfiguration
{
    ClientConfiguration();
    google::cloud::Options options;
    unsigned max_concurrency;
};

}; //namespace runai::llm::streamer::impl::gcs
