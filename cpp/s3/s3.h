#pragma once

#include "common/storage_uri/storage_uri.h"
#include "common/response/response.h"
#include "common/range/range.h"

// For connecting to s3 providers other then aws:
//
// 1. Uri should be in the format s3://bucket/path or gs://bucket/path
//
// 2. Provide endpoint url with environment variable RUNAI_STREAMER_S3_ENDPOINT
//    No need to set the endpoint for gs or aws, if using the default endpoint)
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
// For aws:   <streamer app> s3://bucket/path
// For minio: RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=false AWS_EC2_METADATA_DISABLED=true RUNAI_STREAMER_S3_ENDPOINT="http://localhost:9000" <streamer app> s3://bucket/path
// For gs:    RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=false AWS_EC2_METADATA_DISABLED=true <streamer app> gs://bucket/path

namespace runai::llm::streamer::impl::s3
{

// create client
extern "C" void * runai_create_s3_client(const common::s3::StorageUri & uri);
// destroy client
extern "C" void runai_remove_s3_client(void * client);
// synchronous read
extern "C" common::ResponseCode runai_read_s3_client(void * client, size_t offset, size_t bytesize, char * buffer);
// asynchronous read
extern "C" common::ResponseCode  runai_async_read_s3_client(void * client, unsigned num_ranges, common::Range * ranges, size_t chunk_bytesize, char * buffer);
// wait for asynchronous read response
extern "C" common::ResponseCode  runai_async_response_s3_client(void * client, unsigned * index /* output parameter */);
// list object keys
extern "C" common::ResponseCode runai_list_objects_s3_client(void * client, char*** object_keys, size_t * object_count);
// get size of object in bytes
extern "C" common::ResponseCode runai_object_bytesize_s3_client(void * client, size_t * object_bytesize);

// stop clients
// Stops the responder of each client, in order to notify callers which sent a request and are waiting for a response
extern "C" void runai_stop_s3_clients();
// release clients
extern "C" void runai_release_s3_clients();

}; //namespace runai::llm::streamer::impl::s3
