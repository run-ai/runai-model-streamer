#include "streamer/impl/streamer/streamer.h"

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <cstring>

#include "utils/logging/logging.h"
#include "utils/fd/fd.h"
#include "utils/scope_guard/scope_guard.h"
#include "utils/strings/strings.h"

#include "streamer/impl/batches/batches.h"
#include "common/exception/exception.h"


namespace runai::llm::streamer::impl
{

Streamer::Streamer() : Streamer(Config())
{}

Streamer::Streamer(Config config) :
    _config(std::make_shared<Config>(config)),
    _pool([&](Batch batch, std::atomic<bool> & stopped)
        {
            batch.execute(stopped);
        }, _config->concurrency)
{
    LOG(DEBUG) << config;
}

Streamer::~Streamer()
{
    try
    {
        LOG(DEBUG) << "Streamer shutting down";
    }
    catch(...)
    {}
}

common::ResponseCode Streamer::request(const std::string & path, size_t file_offset, size_t bytesize, void * dst)
{
    LOG(SPAM) << "Requested to read " << bytesize << " bytes from " << path << " offset " << file_offset;

    auto r = request(path, file_offset, bytesize, dst, 1, &bytesize);
    if (r != common::ResponseCode::Success)
    {
        return r;
    }

    return _responder->pop().ret;
}

common::ResponseCode Streamer::request(const std::string & path, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes)
{
    common::ResponseCode ret = common::ResponseCode::Success;

    try
    {
        create_request(path, file_offset, bytesize, dst, num_sizes, internal_sizes);
    }
    catch(const common::Exception & e)
    {
        LOG(ERROR) << "caught exception " << e.what();
        ret = e.error();
    }

    return ret;
}

Streamer::Destination::Destination(const std::string & fs_path) :
    mode(Mode::Path),
    fs_path(fs_path),
    buffer(nullptr)
{}

Streamer::Destination::Destination(void * dst) :
    mode(Mode::Buffer),
    buffer(dst)
{
    if (dst == 0)
    {
        LOG(ERROR) << "Destination buffer is null";
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }
}

void Streamer::create_request(const std::string & path, size_t file_offset, size_t bytesize, const Destination & dst, unsigned num_sizes, size_t * internal_sizes)
{
    LOG(SPAM) << "Requested to read asynchronously " << bytesize << " bytes from " << path << " offset " << file_offset << " in " << num_sizes << " chunks";

    if (bytesize == 0 && num_sizes == 0)
    {
        LOG(ERROR) << "Empty request - no response will be sent";
        throw common::Exception(common::ResponseCode::EmptyRequestError);
    }

    if (num_sizes == 0 || bytesize == 0)
    {
        LOG(ERROR) << "Total bytes to read is " << bytesize << " but number of sub requests is " << num_sizes;
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }

    if (_responder && !_responder->finished())
    {
        LOG(ERROR) << "Previous request is still running";
        throw common::Exception(common::ResponseCode::BusyError);
    }

    // expecting for total of num_sizes responses
    _responder = std::make_shared<common::Responder>(num_sizes);

    // cancel responder in case of an error - cancelled response will not delay sending the next request
    utils::ScopeGuard __responder_release(
                [&](){_responder->cancel();});

    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_unique<common::s3::StorageUri>(path);
        if (_s3 == nullptr)
        {
            _s3_stop = std::make_unique<S3Stop>();
            _s3 = std::make_unique<S3Cleanup>();
        }
    }
    catch(const std::exception& e)
    {
    }

    Batches batches(_config, _responder, path, uri, file_offset, bytesize, dst.buffer, num_sizes, internal_sizes);

    if (batches.total() != bytesize)
    {
        LOG(ERROR) << "Total bytes to read " << bytesize << " is not equal to the sum of the sub ranges, which is " << batches.total();
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }

    if (dst.mode == Destination::Mode::Path)
    {
        batches.set_destination_file(dst.fs_path);
    }

    // send batches to threadpool
    for (unsigned i = 0; i < batches.size(); ++i)
    {
        auto & batch = batches[i];
        if (batch.tasks.size() == 0)
        {
            break;
        }
        LOG(DEBUG) << "sending " << batch.tasks.size() << " tasks to worker " << i << " total bytes " << batch.range.size << " range " << batch.range.start << " to " << batch.range.end;
        _pool.push(std::move(batch));
    }

    __responder_release.cancel();
}

common::Response Streamer::response()
{
    if (_responder == nullptr)
    {
        return common::Response(common::ResponseCode::FinishedError);
    }

    return _responder->pop();
}

// list object keys
common::ResponseCode Streamer::list_objects(const std::string & path, char*** object_keys, size_t * object_count)
{
    std::shared_ptr<common::s3::StorageUri> uri = nullptr;
    try
    {
        uri = std::make_unique<common::s3::StorageUri>(path, common::s3::StorageUri::Type::Path);
        if (_s3 == nullptr)
        {
            _s3_stop = std::make_unique<S3Stop>();
            _s3 = std::make_unique<S3Cleanup>();
        }
    }
    catch(const std::exception& e)
    {
    }

    if (uri.get() != nullptr)
    {
        auto s3_client = std::make_shared<common::s3::S3ClientWrapper>(*uri);
        return s3_client->list_objects(object_keys, object_count);
    }

    // Not object store
    auto response_code = common::ResponseCode::Success;
    try
    {
        auto strings = utils::Fd::list(path);
        utils::Strings::create_cstring_list(strings, object_keys, object_count);
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "failed to list regular files in path " << path;
        response_code = common::ResponseCode::FileAccessError;
    }

    return response_code;
}

// free list of object keys
common::ResponseCode Streamer::free_list_objects(char** object_keys, size_t object_count)
{
    try
    {
        utils::Strings::free_cstring_list(object_keys, object_count);
        return common::ResponseCode::Success;
    }
    catch(const std::exception & e)
    {
        LOG(ERROR) << "Caught exception while releasing list of objects";
    }

    return common::ResponseCode::UnknownError;
}

common::ResponseCode Streamer::request(const std::string & s3_path, const std::string & fs_path)
{
    // verify destination is not s3 uri
    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_unique<common::s3::StorageUri>(fs_path);
    }
    catch(const std::exception& e)
    {
    }

    if (uri != nullptr)
    {
        LOG(ERROR) << "Destination file " << fs_path << " cannot be in object store. Streanmer do not support uploading objects to S3";
        return common::ResponseCode::InvalidParameterError;
    }

    try
    {
        uri = std::make_unique<common::s3::StorageUri>(s3_path);
        if (_s3 == nullptr)
        {
            _s3_stop = std::make_unique<S3Stop>();
            _s3 = std::make_unique<S3Cleanup>();
        }
    }
    catch(const std::exception& e)
    {
    }

    if (uri == nullptr)
    {
        LOG(ERROR) << "Source path " << s3_path << " must be s3 uri";
        return common::ResponseCode::InvalidParameterError;
    }

    // get object size;
    size_t bytesize;

    auto s3_client = std::make_shared<common::s3::S3ClientWrapper>(*uri);
    auto r = s3_client->object_bytesize(&bytesize);

    if (r != common::ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to get header of object " << s3_path;
        return r;
    }

    Destination dst(fs_path);
    create_request(s3_path, 0, bytesize, dst, 1, &bytesize);
    return _responder->pop().ret;
}

}; // namespace runai::llm::streamer::impl
