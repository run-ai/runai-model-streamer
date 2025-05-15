
#include "streamer/impl/workload/workload.h"

#include <memory>
#include <utility>

#include "streamer/impl/s3/s3.h"

#include "common/response_code/response_code.h"
#include "common/exception/exception.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

size_t Workload::size() const
{
    return _batches.size();
}

common::ResponseCode Workload::add_batch(Batch && batch)
{

    if (_batches.size() == 0)
    {
        _params = batch.params;
    }
    else
    {
        return verify_batch(batch.params);
    }

    _batches.push_back(std::move(batch));
    _batches_by_file_index[batch.file_index] = &_batches.back();
    
    return common::ResponseCode::Success;
}

common::ResponseCode Workload::verify_batch(const common::s3::S3ClientWrapper::Params & params)
{
    if (_params.valid() != params.valid())
    {
         LOG(ERROR) << "Workload contains paths of different storage backends";

        return common::ResponseCode::InvalidParameterError;
    }
    if (_params.valid() && params.uri->bucket != _params.uri->bucket)
    {
        LOG(ERROR) << "Workload contains paths of different buckets";
        return common::ResponseCode::InvalidParameterError;
    }

    return common::ResponseCode::Success;
}

void Workload::execute(std::atomic<bool> & stopped)
{
    if (_batches.empty())
    {
        return;
    }

    // create reader
    if (_params.valid())
    {
        async_read(stopped);
    }
    else
    {
        for (auto & batch : _batches)
        {
            batch.execute(stopped);
            LOG(DEBUG) << "Finished batch " << batch;
        }
    }
}

void Workload::async_read(std::atomic<bool> & stopped)
{
    auto response_code = common::ResponseCode::Success;
    try
    {
        const auto & config = *_batches.front().config;
        auto s3_client = std::make_shared<common::s3::S3ClientWrapper>(_params);
        _reader = std::make_shared<S3>(s3_client, config);
        for (auto & batch : _batches)
        {
            LOG(DEBUG) << "Requesting batch " << batch;
            _error_by_file_index[batch.file_index] = common::ResponseCode::Success;
            batch.request(_reader, stopped);
        }

        LOG(DEBUG) << "Waiting for responses";

        // wait for all the batched to finish
        wait_for_responses(stopped);
    }
    catch(const common::Exception & e)
    {
        response_code = e.error();
    }
    catch (...)
    {
        response_code = common::ResponseCode::UnknownError;
    }

    for (auto & batch : _batches)
    {
        const auto error_code = (response_code == common::ResponseCode::Success ?  _error_by_file_index.at(batch.file_index) : response_code);
        batch.handle_error(error_code);
    }
}

void Workload::wait_for_responses(std::atomic<bool> & stopped)
{
    if (stopped)
    {
        throw common::Exception(common::ResponseCode::FinishedError);
    }

    // wait for responses from the reader
    // stopped flag was propagated to the storage backend when the request was made, and the storage backend is responsible for returning FinishedError when stopped flag is set
    while (true)
    {
        auto r = _reader->async_response();

        LOG(SPAM) << "Received response " << r;

        if (r.ret == common::ResponseCode::FinishedError)
        {
            throw common::Exception(common::ResponseCode::FinishedError);
        }

        auto batch = _batches_by_file_index.at(r.file_index);

        if (r.ret != common::ResponseCode::Success)
        {
            _error_by_file_index[r.file_index] = r.ret;
        }

        batch->handle_response(r);
    }

    LOG(DEBUG) << "Finished reading files "  << (stopped ? " - terminated" : " successfully");
}

}; // namespace runai::llm::streamer::impl
