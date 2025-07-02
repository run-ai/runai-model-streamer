#include "gcs/gcs.h"
#include "gcs/client_mgr/client_mgr.h"

#include "common/exception/exception.h"

namespace runai::llm::streamer::impl::gcs
{

common::ResponseCode runai_create_s3_client(const common::s3::Path * path, const common::s3::Credentials_C * credentials, void ** client)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        *client = static_cast<void *>(GCSClientMgr::pop(*path, *credentials));
    }
    catch(const common::Exception & e)
    {
        ret = e.error();
        *client = nullptr;
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to create GCS client";
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
           GCSClientMgr::push(static_cast<GCSClient *>(client));
        }
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove GCS client";
    }
}

void runai_release_s3_clients()
{
    try
    {
        GCSClientMgr::clear();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove all GCS clients";
    }
}

void runai_stop_s3_clients()
{
    try
    {
        GCSClientMgr::stop();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to stop all GCS clients";
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
        auto ptr = static_cast<GCSClient *>(client);
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
            LOG(ERROR) << "Attempt to get read response with null GCS client";
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

        auto ptr = static_cast<GCSClient *>(client);
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

}; // namespace runai::llm::streamer::impl::gcs
