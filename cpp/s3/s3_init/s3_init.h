#pragma once

#include <aws/core/Aws.h>
#include <aws/s3-crt/S3CrtClient.h>
#include <aws/s3-crt/model/BucketLocationConstraint.h>

namespace runai::llm::streamer::impl::s3
{

struct S3Init
{
    S3Init();
    ~S3Init();

    Aws::SDKOptions options;
};

}; //namespace runai::llm::streamer::impl::s3
