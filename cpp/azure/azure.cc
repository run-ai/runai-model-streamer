#include "azure/azure.h"
#include "azure/client/client.h"

#include "common/client_mgr/client_mgr.h"
#include "common/exception/exception.h"
#include "utils/env/env.h"
#include "utils/semver/semver.h"

// For connecting to Azure Blob Storage:
// 1. uri should be in the format az://container/path
// 2. Credentials can be provided via environment variables or config parameters
// 3. See azure.h for detailed credential configuration options

namespace runai::llm::streamer::impl::azure
{

inline constexpr char AzureClientName[] = "Azure";
using AzureClientMgr = common::ClientMgr<AzureClient, AzureClientName>;

// --- Backend API ---

const utils::Semver min_glibc_semver = utils::Semver(common::description(static_cast<int>(common::ResponseCode::GlibcPrerequisite)));
const size_t min_chunk_bytesize = 256 * 1024; // 256 KB minimum for Azure Blob Storage

common::backend_api::ResponseCode_t obj_open_backend(common::backend_api::ObjectBackendHandle_t* out_backend_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;

    try
    {
        // verify prerequisites
        auto glibc_version = utils::get_glibc_version();
        if (min_glibc_semver > glibc_version)
        {
            LOG(ERROR) << "GLIBC version must be at least " << min_glibc_semver << ", instead of " << glibc_version;
            return common::ResponseCode::GlibcPrerequisite;
        }

        size_t chunk_size;
        if (utils::try_getenv("RUNAI_STREAMER_CHUNK_BYTESIZE", chunk_size))
        {
            LOG_IF(INFO, (chunk_size < min_chunk_bytesize)) << "Minimal chunk size to read from Azure is 256 KiB";
        }

        // Azure SDK doesn't require global initialization like AWS SDK
        LOG(INFO) << "Azure backend initialized successfully";
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to init Azure backend: " << e.what();
        ret = common::ResponseCode::FileAccessError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_close_backend(common::backend_api::ObjectBackendHandle_t backend_handle)
{
    // Azure SDK cleanup is automatic
    common::ResponseCode ret = common::ResponseCode::Success;
    return ret;
}

common::backend_api::ObjectShutdownPolicy_t obj_get_backend_shutdown_policy()
{
    return common::backend_api::OBJECT_SHUTDOWN_POLICY_ON_PROCESS_EXIT;
}

// --- Client API ---

common::backend_api::ResponseCode_t obj_create_client(common::backend_api::ObjectBackendHandle_t backend_handle,
                                                       const common::backend_api::ObjectClientConfig_t* client_initial_config,
                                                       common::backend_api::ObjectClientHandle_t* out_client_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        *out_client_handle = AzureClientMgr::pop(*client_initial_config);
    }
    catch(const common::Exception & e)
    {
        ret = e.error();
        *out_client_handle = nullptr;
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to create Azure client: " << e.what();
        ret = common::ResponseCode::FileAccessError;
        *out_client_handle = nullptr;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_remove_client(common::backend_api::ObjectClientHandle_t client_handle)
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        if (client_handle)
        {
            AzureClientMgr::push(static_cast<AzureClient *>(client_handle));
        }
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove Azure client: " << e.what();
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_remove_all_clients()
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        AzureClientMgr::clear();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to remove all Azure clients: " << e.what();
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_cancel_all_reads()
{
    common::ResponseCode ret = common::ResponseCode::Success;
    try
    {
        AzureClientMgr::stop();
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Failed to stop all Azure clients: " << e.what();
        ret = common::ResponseCode::UnknownError;
    }
    return ret;
}

common::backend_api::ResponseCode_t obj_request_read(common::backend_api::ObjectClientHandle_t client_handle,
                                                      const char* path,
                                                      common::backend_api::ObjectRange_t range,
                                                      char* destination_buffer,
                                                      common::backend_api::ObjectRequestId_t request_id)
{
    auto client = static_cast<AzureClient *>(client_handle);
    if (client == nullptr)
    {
        LOG(ERROR) << "Azure client handle is null";
        return common::ResponseCode::InvalidParameterError;
    }

    return client->async_read(path, range, destination_buffer, request_id);
}

common::backend_api::ResponseCode_t obj_wait_for_completions(common::backend_api::ObjectClientHandle_t client_handle,
                                                             common::backend_api::ObjectCompletionEvent_t* event_buffer,
                                                             unsigned int max_events_to_retrieve,
                                                             unsigned int* out_num_events_retrieved,
                                                             common::backend_api::ObjectWaitMode_t wait_mode)
{
    auto client = static_cast<AzureClient *>(client_handle);
    if (client == nullptr)
    {
        LOG(ERROR) << "Azure client handle is null";
        return common::ResponseCode::InvalidParameterError;
    }

    *out_num_events_retrieved = 0;

    for (unsigned int i = 0; i < max_events_to_retrieve; ++i)
    {
        auto response = client->async_read_response();
        
        // Always store the response in the event buffer (matching S3/GCS behavior)
        event_buffer[i].request_id = response.handle;
        event_buffer[i].response_code = response.ret;
        event_buffer[i].bytes_transferred = 0;
        (*out_num_events_retrieved)++;
        
        // Break after storing FinishedError so caller can see it
        if (response.ret == common::ResponseCode::FinishedError)
        {
            break;
        }

        // For non-blocking mode, exit after first attempt
        if (wait_mode == common::backend_api::OBJECT_WAIT_MODE_NON_BLOCKING && i == 0)
        {
            break;
        }
    }

    return common::ResponseCode::Success;
}

}; // namespace runai::llm::streamer::impl::azure
