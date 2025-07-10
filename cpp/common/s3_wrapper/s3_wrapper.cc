#include <regex>
#include <vector>
#include <chrono>

#include "common/s3_wrapper/s3_wrapper.h"
#include "common/s3_credentials/s3_credentials.h"
#include "common/exception/exception.h"

#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::s3
{

std::mutex S3ClientWrapper::_backend_handle_mutex;

S3ClientWrapper::Params::Params(std::shared_ptr<StorageUri> uri, const Credentials & credentials, size_t chunk_bytesize) :
    uri(uri),
    credentials(credentials)
{
    if (credentials.endpoint.has_value()) // endpoint passed as parameter by user application (in credentials)
    {
        _endpoint = credentials.endpoint.value();
        LOG(DEBUG) <<"Using credentials endpoint " << credentials.endpoint.value();
    }
    else
    {
        std::string endpoint;
        bool override_endpoint = utils::try_getenv("AWS_ENDPOINT_URL", endpoint);
        bool override_endpoint_flag = utils::getenv<bool>("RUNAI_STREAMER_OVERRIDE_ENDPOINT_URL", true);
        if (override_endpoint) // endpoint passed as environment variable
        {
            LOG(DEBUG) << "direct override of url endpoint in client configuration";
            if (override_endpoint_flag)
            {
                _endpoint = endpoint;
            }
            LOG(DEBUG) <<"Using environment variable endpoint " << endpoint << (override_endpoint_flag ? " , using configuration parameter endpointOverride" : "");
        }
    }

    config.endpoint_url = _endpoint.empty() ? nullptr : _endpoint.c_str();

    credentials.to_object_client_config(_initial_params);

    config.num_initial_params = _initial_params.size();
    config.initial_params = _initial_params.data();
    config.default_storage_chunk_size = chunk_bytesize;
}

const utils::Semver min_glibc_semver = utils::Semver(description(static_cast<int>(ResponseCode::GlibcPrerequisite)));

S3ClientWrapper::BackendHandle::BackendHandle(const Params & params) :
    dylib_ptr(open_object_storage_impl(params))
{
    ASSERT(dylib_ptr != nullptr) << "Failed to open libstreamers3.so"; // should never happen

    auto __open_object_storage = dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectBackendHandle_t*)>("obj_open_backend");
    auto ret = __open_object_storage(&_backend_handle);
    if (ret != ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to open object storage";
        throw Exception(ret);
    }
    LOG(DEBUG) << "Opened object storage";
}

S3ClientWrapper::BackendHandle::~BackendHandle()
{
    try
    {
        auto __close_object_storage = dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectBackendHandle_t)>("obj_close_backend");
        auto ret = __close_object_storage(_backend_handle);
        if (ret != ResponseCode::Success)
        {
            LOG(ERROR) << "Failed to close object storage";
            throw Exception(ret);
        }
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while closing object storage";
    }
}

std::shared_ptr<utils::Dylib> S3ClientWrapper::BackendHandle::open_object_storage_impl(const Params & params)
{
    // lazy load the s3 library once
    try
    {
        return std::make_shared<utils::Dylib>("libstreamers3.so");
    }
    catch (...)
    {
        LOG(ERROR) << "Failed to open libstreamers3.so";
    }
    throw Exception(ResponseCode::S3NotSupported);
    return nullptr;
}

common::backend_api::ObjectBackendHandle_t S3ClientWrapper::BackendHandle::backend_handle() const
{
    return _backend_handle;
}

void S3ClientWrapper::shutdown()
{
    try
    {
        LOG(DEBUG) << "Shutting down S3 client wrapper";
        std::shared_ptr<BackendHandle> handle = manage_backend_handle(Params(), ManageBackendHandleOp::CREATE);
        auto __release_s3_clients = handle->dylib_ptr->dlsym<common::backend_api::ResponseCode_t(*)()>("obj_remove_all_clients");
        __release_s3_clients();

       // destroy according to backend shutdown policy
       manage_backend_handle(Params(), ManageBackendHandleOp::DESTROY);
    }
    catch(...)
    {
    }
}

void S3ClientWrapper::stop()
{
    try
    {
        std::shared_ptr<BackendHandle> handle = manage_backend_handle(Params(), ManageBackendHandleOp::CREATE);
        ASSERT(handle != nullptr) << "Backend handle is already closed";
        auto __stop_s3_clients = handle->dylib_ptr->dlsym<common::backend_api::ResponseCode_t(*)()>("obj_cancel_all_reads");
        __stop_s3_clients();
    }
    catch(...)
    {
    }
}

S3ClientWrapper::S3ClientWrapper(const Params & params) :
    _backend_handle(manage_backend_handle(params, ManageBackendHandleOp::CREATE)),
    _s3_client(create_client(params))
{
    LOG(SPAM) << "Created client for uri " << *params.uri;
    ASSERT(_backend_handle != nullptr) << "Backend handle is alreday closed";
}

S3ClientWrapper::~S3ClientWrapper()
{
    try
    {
        ASSERT(_backend_handle != nullptr) << "Backend handle is alreday closed";
        auto __s3_remove = _backend_handle->dylib_ptr->dlsym<common::backend_api::ResponseCode_t(*)(common::backend_api::ObjectClientHandle_t)>("obj_remove_client");
        __s3_remove(_s3_client);
    }
    catch(...)
    {
        LOG(ERROR) << "Caught exception while deleting s3 client";
    }
}


std::shared_ptr<S3ClientWrapper::BackendHandle> S3ClientWrapper::manage_backend_handle(const Params & params, ManageBackendHandleOp op)
{
    std::unique_lock<std::mutex> lock(_backend_handle_mutex);
    LOG(SPAM) << "Managing backend handle " << (op == ManageBackendHandleOp::CREATE ? "CREATE" : "DESTROY");

    static auto __backend_handle = std::make_shared<BackendHandle>(params);
    if (__backend_handle == nullptr && op == ManageBackendHandleOp::CREATE)
    {
        LOG(DEBUG) << "Backend handle is not initialized";
        __backend_handle = std::make_shared<BackendHandle>(params);
    }

    if (op == ManageBackendHandleOp::DESTROY && __backend_handle != nullptr)
    {
        const auto shutdown_policy = get_backend_shutdown_policy(__backend_handle);
        if (shutdown_policy == common::backend_api::ObjectShutdownPolicy_t::OBJECT_SHUTDOWN_POLICY_ON_STREAMER_SHUTDOWN)
        {
            LOG(DEBUG) << "Destroying backend handle on streamer shutdown";
            ASSERT(__backend_handle != nullptr) << "Backend handle is not initialized";
            __backend_handle = nullptr;
        }
        else if (shutdown_policy == common::backend_api::ObjectShutdownPolicy_t::OBJECT_SHUTDOWN_POLICY_ON_PROCESS_EXIT)
        {
            LOG(DEBUG) << "Object storage backend will be closed on process exit";
        }
        else
        {
            LOG(ERROR) << "Unknown object storage backend shutdown policy";
        }
    }
    return __backend_handle;
}

void * S3ClientWrapper::create_client(const Params & params)
{
    auto __s3_gen = _backend_handle->dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectBackendHandle_t, const common::backend_api::ObjectClientConfig_t*, common::backend_api::ObjectClientHandle_t*)>("obj_create_client");
    auto start = std::chrono::steady_clock::now();

    common::backend_api::ObjectClientHandle_t client;

    auto ret = __s3_gen(_backend_handle->backend_handle(), &params.config, &client);
    if (ret != ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to create S3 client for uri " << *params.uri << " and endpoint " << params.config.endpoint_url;
        throw Exception(ret);
    }
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    LOG(SPAM) << "created s3 client in " << duration.count() << " ms";
    return client;
}

ResponseCode S3ClientWrapper::async_read(const Params & params, common::backend_api::ObjectRequestId_t request_id, const Range & range, char * buffer)
{
    auto _s3_async_read = _backend_handle->dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectClientHandle_t, const char*, common::backend_api::ObjectRange_t, char*, common::backend_api::ObjectRequestId_t)>("obj_request_read");
    return _s3_async_read(_s3_client, params.uri->uri.c_str(), range.to_backend_api_range(), buffer, request_id);
    return common::ResponseCode::Success;
}

common::ResponseCode S3ClientWrapper::async_read_response(std::vector<backend_api::ObjectCompletionEvent_t> & event_buffer, unsigned max_events_to_retrieve)
{
    if (max_events_to_retrieve == 0)
    {
        LOG(WARNING) << "Max events to retrieve is 0";
        return common::ResponseCode::Success;
    }

    event_buffer.resize(max_events_to_retrieve);
    unsigned int out_num_events_retrieved;
    auto _s3_async_response = _backend_handle->dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectClientHandle_t, common::backend_api::ObjectCompletionEvent_t*, unsigned int, unsigned int*, common::backend_api::ObjectWaitMode_t)>("obj_wait_for_completions");
    auto ret = _s3_async_response(_s3_client, event_buffer.data(), max_events_to_retrieve, &out_num_events_retrieved, common::backend_api::OBJECT_WAIT_MODE_BLOCK);

    if (ret == common::ResponseCode::Success)
    {
        ASSERT(out_num_events_retrieved >= 0 && out_num_events_retrieved <= max_events_to_retrieve);
        event_buffer.resize(out_num_events_retrieved);
    }
    return ret;
}

common::backend_api::ObjectShutdownPolicy_t S3ClientWrapper::get_backend_shutdown_policy(std::shared_ptr<S3ClientWrapper::BackendHandle> handle)
{
    ASSERT(handle != nullptr) << "object storage backend handle is not initialized";
    auto __get_backend_shutdown_policy = handle->dylib_ptr->dlsym<common::backend_api::ObjectShutdownPolicy_t(*)()>("obj_get_backend_shutdown_policy");
    return __get_backend_shutdown_policy();
}

}; // namespace runai::llm::streamer::common::s3
