#pragma once

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/BucketLocationConstraint.h>


#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::impl::s3
{

struct ClientConfiguration
{
    ClientConfiguration();
    Aws::S3Crt::ClientConfiguration config;
};

}; //namespace runai::llm::streamer::impl::s3
