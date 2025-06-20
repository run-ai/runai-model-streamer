#include <regex>
#include <vector>
#include <chrono>

#include "common/s3_wrapper/s3_wrapper.h"
#include "common/path/path.h"
#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::common::s3
{

const utils::Semver min_glibc_semver = utils::Semver(description(static_cast<int>(ResponseCode::GlibcPrerequisite)));

void S3ClientWrapper::shutdown()
{
    try
    {
        std::shared_ptr<utils::Dylib> s3_dylib(open_s3());
        static auto __release_s3_clients = s3_dylib->dlsym<void(*)()>("runai_release_s3_clients");
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
        std::shared_ptr<utils::Dylib> s3_dylib(open_s3());
        static auto __stop_s3_clients = s3_dylib->dlsym<void(*)()>("runai_stop_s3_clients");
        __stop_s3_clients();
    }
    catch(...)
    {
    }
}

S3ClientWrapper::S3ClientWrapper(const Params & params) :
    _s3_dylib(open_s3()),
    _s3_client(create_client(*params.uri, params.credentials))
{
    LOG(SPAM) << "Created client for uri " << *params.uri;
}

S3ClientWrapper::~S3ClientWrapper()
{
    try
    {
        static auto __s3_remove = _s3_dylib->dlsym<void(*)(void *)>("runai_remove_s3_client");
        __s3_remove(_s3_client);
    }
    catch(...)
    {
        LOG(ERROR) << "Caught exception while deleting s3 client";
    }
}

std::shared_ptr<utils::Dylib> S3ClientWrapper::open_s3()
{
    // lazy load the s3 library once
    try
    {
        static auto __s3_dylib = open_s3_impl();
        return __s3_dylib;
    }
    catch (...)
    {
        LOG(ERROR) << "Failed to open libstreamers3.so";
    }
    throw Exception(ResponseCode::S3NotSupported);
    return nullptr;
}

std::shared_ptr<utils::Dylib> S3ClientWrapper::open_s3_impl()
{
    // verify prerequisites
    auto glibc_version = utils::get_glibc_version();
    if (min_glibc_semver > glibc_version)
    {
        LOG(ERROR) << "GLIBC version must be at least " << min_glibc_semver << ", instead of " << glibc_version;
        throw Exception(ResponseCode::GlibcPrerequisite);
    }

    size_t chunk_size;
    if (utils::try_getenv("RUNAI_STREAMER_CHUNK_BYTESIZE", chunk_size))
    {
        LOG_IF(INFO, (chunk_size < min_chunk_bytesize)) << "Minimal chunk size to read from S3 is 5 MiB";
    }

    return std::make_shared<utils::Dylib>("libstreamers3.so");
}

void * S3ClientWrapper::create_client(const StorageUri & uri, const Credentials & credentials)
{
    static auto __s3_gen = _s3_dylib->dlsym<ResponseCode(*)(const Path *, const Credentials_C *, void **)>("runai_create_s3_client");
    auto start = std::chrono::steady_clock::now();

    void * client;
    const Path s3_path(uri);
    Credentials_C creds(credentials);
    auto ret = __s3_gen(&s3_path, &creds, &client);
    if (ret != ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to create S3 client for uri " << uri;
        throw Exception(ret);
    }
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    LOG(SPAM) << "created s3 client in " << duration.count() << " ms";
    return client;
}

ResponseCode S3ClientWrapper::async_read(const Params & params, common::backend_api::ObjectRequestId_t request_id, const Range & range, size_t chunk_bytesize, char * buffer)
{
    static auto _s3_async_read = _s3_dylib->dlsym<ResponseCode(*)(void *, common::backend_api::ObjectRequestId_t, const Path *, const Range *, size_t, char *)>("runai_async_read_s3_client");
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
    static auto _s3_async_response = _s3_dylib->dlsym<ResponseCode(*)(void *, common::backend_api::ObjectCompletionEvent_t*, unsigned, unsigned*)>("runai_async_response_s3_client");
    auto ret = _s3_async_response(_s3_client, event_buffer.data(), max_events_to_retrieve, &out_num_events_retrieved);

    if (ret == common::ResponseCode::Success)
    {
        ASSERT(out_num_events_retrieved >= 0 && out_num_events_retrieved <= max_events_to_retrieve);
        event_buffer.resize(out_num_events_retrieved);
    }
    return ret;
}

}; // namespace runai::llm::streamer::common::s3
