#pragma once

#include "common/storage_uri/storage_uri.h"
#include "common/response/response.h"
#include "common/range/range.h"

// For connecting to s3 providers other then aws:
//
// 1. Uri should be in the format s3://bucket/path
//
// 2. Provide endpoint url only with environment variable AWS_ENDPOINT_URL
//    Note: the aws cpp sdk ignores the endpoint url in ~/.aws/config file
//    No need to set the endpoint for aws, if using the default endpoint
//
// 3. Set RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING = false
//    This parameter controls the parsing of the URL
//    By setting this option, you control whether the SDK uses virtual-hosted–style URLs (i.e., bucketname.s3.amazonaws.com) or path-style URLs (i.e., s3.amazonaws.com/bucketname).
//
// 4. Set environment variable AWS_EC2_METADATA_DISABLED = true
//    See https://github.com/aws/aws-sdk-cpp/issues/1410
//    This is because the Aws::ClientConfiguration c'tor tries to retrieve the region from the aws server, which causes a 5 seconds delay
//    The ClientConfigurationProvider needs to be created before creating ClientConfiguration and S3Client in order to set the environment variable
//    Then the ClientConfigurationProvider object should be destructed for restoring the environment variable
//    Note: this workaround should be removed if the Aws::S3Crt::ClientConfiguration will accept shouldDisableIMDS
//
// 5. Credentials can be provided in ~/.aws/credentials file or by passing environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
//
// aws:   <streamer app> s3://bucket/path
// gcs:   AWS_ENDPOINT_URL="https://storage.googleapis.com" RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=false AWS_EC2_METADATA_DISABLED=true <streamer app> s3://bucket/path
// minio: AWS_ENDPOINT_URL="http://localhost:9000" RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=0 AWS_EC2_METADATA_DISABLED=true AWS_ACCESS_KEY_ID="minio" AWS_SECRET_ACCESS_KEY="miniostorage" <streamer app> s3://bucket/path

namespace runai::llm::streamer::impl::s3
{

// create client
extern "C" void * runai_create_s3_client(const common::s3::StorageUri & uri);
// destroy client
extern "C" void runai_remove_s3_client(void * client);
// asynchronous read
extern "C" common::ResponseCode  runai_async_read_s3_client(void * client, unsigned num_ranges, common::Range * ranges, size_t chunk_bytesize, char * buffer);
// wait for asynchronous read response
extern "C" common::ResponseCode  runai_async_response_s3_client(void * client, unsigned * index /* output parameter */);
// stop clients
// Stops the responder of each client, in order to notify callers which sent a request and are waiting for a response
extern "C" void runai_stop_s3_clients();
// release clients
extern "C" void runai_release_s3_clients();

}; //namespace runai::llm::streamer::impl::s3
