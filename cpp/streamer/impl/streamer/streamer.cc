#include "streamer/impl/streamer/streamer.h"

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "utils/logging/logging.h"
#include "utils/scope_guard/scope_guard.h"

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

void Streamer::create_request(const std::string & path, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes)
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

    if (dst == 0)
    {
        LOG(ERROR) << "Destination buffer is null";
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
    utils::ScopeGuard __responder_release([&](){_responder->cancel();});

    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_unique<common::s3::StorageUri>(path);
    }
    catch(const std::exception& e)
    {
    }

    if (uri != nullptr && _s3 == nullptr)
    {
        // adjust fd limit acording to concurrency
        auto fd_limit = utils::get_cur_file_descriptors();
        LOG(DEBUG) << "Process file descriptors limit is " << fd_limit << " and concurrency level is " << _config->concurrency;
        const auto desired_fd_limit = _config->concurrency * 64;
        if (fd_limit < desired_fd_limit)
        {
            if (desired_fd_limit > utils::get_max_file_descriptors())
            {
                LOG(ERROR) << "Insufficient file descriptors limit " << fd_limit << " for concurrency level " << _config->concurrency << " ; increase fd limit to " << desired_fd_limit << " or higher, depending on your application fd usage";
                throw common::Exception(common::ResponseCode::InsufficientFdLimit);
            }
            LOG(INFO) << "Increasing fd soft limit to " << desired_fd_limit << " for concurrency level " << _config->concurrency;
            _fd_limit = std::make_unique<utils::FdLimitSetter>(desired_fd_limit);
        }
        _s3_stop = std::make_unique<S3Stop>();
        _s3 = std::make_unique<S3Cleanup>();
    }

    Batches batches(_config, _responder, path, uri, file_offset, bytesize, dst, num_sizes, internal_sizes);

    if (batches.total() != bytesize)
    {
        LOG(ERROR) << "Total bytes to read " << bytesize << " is not equal to the sum of the sub ranges, which is " << batches.total();
        throw common::Exception(common::ResponseCode::InvalidParameterError);
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

}; // namespace runai::llm::streamer::impl
