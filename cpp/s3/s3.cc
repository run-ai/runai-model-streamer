#include "s3/s3.h"

#include <vector>
#include <string>

#include "utils/strings/strings.h"

#include "s3/client_mgr/client_mgr.h"

// For connecting to s3 providers other then aws:
// 1. uri should be in the format s3://bucket/path or gs://bucket/path
// 2. endpoint url is provided with environment variable RUNAI_STREAMER_S3_ENDPOINT (no need to set the endpoint for gs or aws, if using the default endpoint)
// 3. set environment variable AWS_EC2_METADATA_DISABLED = true
// 4. set RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING = false

// e.g. for gs: RUNAI_STREAMER_S#_ENDPOINT="https://storage.googleapis.com" RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=false AWS_EC2_METADATA_DISABLED=true <streamer app> s3://bucket/path

// Important: for s3 compatibale (e.g. minio) run the streamer with AWS_EC2_METADATA_DISABLED=true - see https://github.com/aws/aws-sdk-cpp/issues/1410
// This is because the Aws::ClientConfiguration c'tor tries to retrieve the region from the aws server, which causes a 5 seconds delay
// The ClientConfigurationProvider needs to be created before creating ClientConfiguration and S3Client in order to set the environment variable
// Then the ClientConfigurationProvider object should be destructed for restoring the environment variable
// Note: this workaround should be removed if the Aws::S3Crt::ClientConfiguration will accept shouldDisableIMDS

namespace runai::llm::streamer::impl::s3
{

void * runai_create_s3_client(const common::s3::StorageUri & uri)
{
    try
    {
        return static_cast<void *>(S3ClientMgr::pop(uri));
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to create S3 client";
    }
    return nullptr;
}

void runai_remove_s3_client(void * client)
{
    try
    {
        if (client)
        {
           S3ClientMgr::push(static_cast<S3Client *>(client));
        }
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove S3 client";
    }
}

void runai_release_s3_clients()
{
    try
    {
        S3ClientMgr::clear();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove all S3 clients";
    }
}

void runai_stop_s3_clients()
{
    try
    {
        S3ClientMgr::stop();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to stop all S3 clients";
    }
}

common::ResponseCode runai_read_s3_client(void * client, size_t offset, size_t bytesize, char * buffer)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to read with null s3 client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<S3Client *>(client);
        return ptr->read(offset, bytesize, buffer);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending sync read request";
    }
    return common::ResponseCode::UnknownError;
}

common::ResponseCode  runai_async_read_s3_client(void * client, unsigned num_ranges, common::Range * ranges, size_t chunk_bytesize, char * buffer)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to read with null s3 client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<S3Client *>(client);
        return ptr->async_read(num_ranges, ranges, chunk_bytesize, buffer);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

common::ResponseCode runai_async_response_s3_client(void * client, unsigned * index)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to get read response with null s3 client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<S3Client *>(client);
        auto response = ptr->async_read_response();
        *index = response.index;
        return response.ret;
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

common::ResponseCode runai_list_objects_s3_client(void * client, char*** object_keys, size_t * object_count)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to list objects with null s3 client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<S3Client *>(client);
        std::vector<std::string> strings;
        auto response = ptr->list(strings);
        if (response == common::ResponseCode::Success)
        {
            utils::Strings::create_cstring_list(strings, object_keys, object_count);
        }
        return response;
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while requesting list of objects";
    }

    return common::ResponseCode::UnknownError;
}

common::ResponseCode runai_object_bytesize_s3_client(void * client, size_t * object_bytesize)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to get object size with null s3 client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<S3Client *>(client);
        return ptr->bytesize(object_bytesize);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while requesting object size";
    }

    return common::ResponseCode::UnknownError;
}

}; // namespace runai::llm::streamer::impl::s3
