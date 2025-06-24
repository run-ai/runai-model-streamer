#include <regex>
#include <vector>
#include <chrono>

#include "common/s3_wrapper/s3_wrapper.h"
#include "common/s3_credentials/s3_credentials.h"
#include "common/path/path.h"
#include "common/exception/exception.h"

#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::s3
{

S3ClientWrapper::Params::Params(std::shared_ptr<StorageUri> uri, const Credentials & credentials) :
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
}

const utils::Semver min_glibc_semver = utils::Semver(description(static_cast<int>(ResponseCode::GlibcPrerequisite)));

S3ClientWrapper::BackendHandle::BackendHandle(const Params & params) :
    dylib_ptr(open_object_storage_impl(params))
{
    ASSERT(dylib_ptr != nullptr) << "Failed to open libstreamers3.so"; // should never happen

    static auto __open_object_storage = dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectBackendHandle_t*)>("obj_open_backend");
    auto ret = __open_object_storage(&_backend_handle);
    if (ret != ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to open object storage";
        throw Exception(ret);
    }
}

S3ClientWrapper::BackendHandle::~BackendHandle()
{
    try
    {
        static auto __close_object_storage = dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectBackendHandle_t)>("obj_close_backend");
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
        std::shared_ptr<BackendHandle> handle = create_backend_handle(Params());
        static auto __release_s3_clients = handle->dylib_ptr->dlsym<void(*)()>("runai_release_s3_clients");
        __release_s3_clients();
    }
    catch(...)
    {
    }
}

void S3ClientWrapper::stop()
{
    try
    {
        std::shared_ptr<BackendHandle> handle = create_backend_handle(Params());
        static auto __stop_s3_clients = handle->dylib_ptr->dlsym<void(*)()>("runai_stop_s3_clients");
        __stop_s3_clients();
    }
    catch(...)
    {
    }
}

S3ClientWrapper::S3ClientWrapper(const Params & params) :
    _backend_handle(create_backend_handle(params)),
    _s3_client(create_client(params))
{
    LOG(SPAM) << "Created client for uri " << *params.uri;
}

S3ClientWrapper::~S3ClientWrapper()
{
    try
    {
        static auto __s3_remove = _backend_handle->dylib_ptr->dlsym<void(*)(void *)>("runai_remove_s3_client");
        __s3_remove(_s3_client);
    }
    catch(...)
    {
        LOG(ERROR) << "Caught exception while deleting s3 client";
    }
}


std::shared_ptr<S3ClientWrapper::BackendHandle> S3ClientWrapper::create_backend_handle(const Params & params)
{
    // the backend is closed when the process exits
    static auto __backend_handle = std::make_shared<BackendHandle>(params);
    return __backend_handle;
}

void * S3ClientWrapper::create_client(const Params & params)
{
    static auto __s3_gen = _backend_handle->dylib_ptr->dlsym<ResponseCode(*)(common::backend_api::ObjectBackendHandle_t, const common::backend_api::ObjectClientConfig_t*, common::backend_api::ObjectClientHandle_t*)>("obj_create_client");
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

ResponseCode S3ClientWrapper::async_read(const Params & params, common::backend_api::ObjectRequestId_t request_id, const Range & range, size_t chunk_bytesize, char * buffer)
{
    static auto _s3_async_read = _backend_handle->dylib_ptr->dlsym<ResponseCode(*)(void *, common::backend_api::ObjectRequestId_t, const Path *, const Range *, size_t, char *)>("runai_async_read_s3_client");
    const Path s3_path(*params.uri);
    return _s3_async_read(_s3_client, request_id, &s3_path, &range, chunk_bytesize, buffer);
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
    static auto _s3_async_response = _backend_handle->dylib_ptr->dlsym<ResponseCode(*)(void *, common::backend_api::ObjectCompletionEvent_t*, unsigned, unsigned*)>("runai_async_response_s3_client");
    auto ret = _s3_async_response(_s3_client, event_buffer.data(), max_events_to_retrieve, &out_num_events_retrieved);

    if (ret == common::ResponseCode::Success)
    {
        ASSERT(out_num_events_retrieved >= 0 && out_num_events_retrieved <= max_events_to_retrieve);
        event_buffer.resize(out_num_events_retrieved);
    }
    return ret;
}

}; // namespace runai::llm::streamer::common::s3
