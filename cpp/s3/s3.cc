#include "s3/s3.h"
#include "s3/client_mgr/client_mgr.h"

#include "common/exception/exception.h"

// For connecting to s3 providers other then aws:
// 1. uri should be in the format s3://bucket/path
// 2. endpoint url must be provided with environment variable AWS_ENDPOINT_URL
// 3. set environment variable AWS_EC2_METADATA_DISABLED = true
// 4. set RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING = false
// 5. Credentials can be provided in ~/.aws/credentials file or by passing environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY

// e.g. gcs: AWS_ENDPOINT_URL="https://storage.googleapis.com" RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=false AWS_EC2_METADATA_DISABLED=true <streamer app> s3://bucket/path
// e.g. minio: AWS_ENDPOINT_URL="http://localhost:9000" RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=0 AWS_EC2_METADATA_DISABLED=true AWS_ACCESS_KEY_ID="minio" AWS_SECRET_ACCESS_KEY="miniostorage"

// Important: for s3 compatibale (e.g. minio) run the streamer with AWS_EC2_METADATA_DISABLED=true - see https://github.com/aws/aws-sdk-cpp/issues/1410
// This is because the Aws::ClientConfiguration c'tor tries to retrieve the region from the aws server, which causes a 5 seconds delay
// The ClientConfigurationProvider needs to be created before creating ClientConfiguration and S3Client in order to set the environment variable
// Then the ClientConfigurationProvider object should be destructed for restoring the environment variable
// Note: this workaround should be removed if the Aws::S3Crt::ClientConfiguration will accept shouldDisableIMDS

namespace runai::llm::streamer::impl::s3
{

common::ResponseCode runai_create_s3_client(const common::s3::Path * path, const common::s3::Credentials_C * credentials, void ** client)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        *client = static_cast<void *>(S3ClientMgr::pop(*path, *credentials));
    }
    catch(const common::Exception & e)
    {
        ret = e.error();
        *client = nullptr;
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to create S3 client";
        ret = common::ResponseCode::FileAccessError;
        *client = nullptr;
    }
    return ret;
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

common::ResponseCode runai_async_read_s3_client(void * client, common::backend_api::ObjectRequestId_t request_id, const common::s3::Path * path, common::Range * range, size_t chunk_bytesize, char * buffer)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to read with null s3 client";
            return common::ResponseCode::UnknownError;
        }
        auto ptr = static_cast<S3Client *>(client);
        return ptr->async_read(*path, request_id, *range, chunk_bytesize, buffer);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

common::ResponseCode runai_async_response_s3_client(void * client,
                                                    common::backend_api::ObjectCompletionEvent_t* event_buffer,
                                                    unsigned int max_events_to_retrieve,
                                                    unsigned int* out_num_events_retrieved)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to get read response with null s3 client";
            return common::ResponseCode::UnknownError;
        }
        if (max_events_to_retrieve == 0)
        {
            LOG(ERROR) << "Attempt to get read response with max_events_to_retrieve = 0";
            return common::ResponseCode::UnknownError;
        }
        if (!event_buffer || !out_num_events_retrieved)
        {
            LOG(ERROR) << "Attempt to get read response with null event_buffer or out_num_events_retrieved";
            return common::ResponseCode::UnknownError;
        }

        // for now reads a single event

        auto ptr = static_cast<S3Client *>(client);
        auto response = ptr->async_read_response();
        *out_num_events_retrieved = 1;
        event_buffer[0].request_id = response.handle;
        event_buffer[0].response_code = response.ret;
        return common::ResponseCode::Success;
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while sending async request";
    }
    return common::ResponseCode::UnknownError;
}

}; // namespace runai::llm::streamer::impl::s3
