
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
    return _batches_by_file_index.size();
}

common::ResponseCode Workload::add_batch(Batch && batch)
{
    const auto file_index = batch.file_index;
    ASSERT(_batches_by_file_index.find(file_index) == _batches_by_file_index.end()) << "Batch for file index " << file_index << " already exists";

    if (size() == 0)
    {
        _params = batch.params;
    }
    else if  (auto res = verify_batch(batch.params); res != common::ResponseCode::Success)
    {
        return res;
    }

    _batches_by_file_index.emplace(file_index, std::move(batch));

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
    if (size() == 0)
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
        for (auto & [file_index, batch] : _batches_by_file_index)
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
        const auto & config = _batches_by_file_index.begin()->second.config;

        auto s3_client = std::make_shared<common::s3::S3ClientWrapper>(_params);
        _reader = std::make_shared<S3>(s3_client, *config);

        unsigned requested_batches = 0;
        for (auto & [file_index, batch] : _batches_by_file_index)
        {
            _error_by_file_index[file_index] = handle_batch(file_index, batch, stopped);
            requested_batches += (_error_by_file_index[file_index] == common::ResponseCode::Success ? 1 : 0);
        }

        // wait for all the batched to finish
        if (requested_batches > 0)
        {
            LOG(DEBUG) << "Waiting for responses";
            wait_for_responses(stopped);
        }
    }
    catch(const common::Exception & e)
    {
        if (e.error() != common::ResponseCode::FinishedError)
        {
            LOG(ERROR) << "Error " << e.error() << " while reading batches";
        }
        response_code = e.error();
    }
    catch (...)
    {
        LOG(ERROR) << "Unknown error while reading batches";
        response_code = common::ResponseCode::UnknownError;
    }

    for (auto & [file_index, batch] : _batches_by_file_index)
    {
        auto error_code = response_code;
        if (error_code == common::ResponseCode::Success)
        {
            ASSERT(_error_by_file_index.find(file_index) != _error_by_file_index.end()) << "Error by file index " << file_index << " not found";
            error_code = _error_by_file_index.at(file_index);
        }

        batch.handle_error(error_code);
    }
}

common::ResponseCode Workload::handle_batch(unsigned file_index, Batch & batch, std::atomic<bool> & stopped)
{
    LOG(SPAM) << "Requesting batch " << batch;
    auto batch_response_code = common::ResponseCode::Success;

    try
    {
        batch.request(_reader, stopped);
    }
    catch(const common::Exception & e)
    {
        LOG(ERROR) << "Error " << e.error() << " while requesting batch " << batch;
        batch_response_code = e.error();
    }
    catch (...)
    {
        LOG(ERROR) << "Unknown error while requesting batch " << batch;
        batch_response_code = common::ResponseCode::UnknownError;
    }

    return batch_response_code;
}

void Workload::wait_for_responses(std::atomic<bool> & stopped)
{
    if (stopped)
    {
        LOG(DEBUG) << "Terminated while waiting for responses";
        throw common::Exception(common::ResponseCode::FinishedError);
    }

    // wait for responses from the reader
    // stopped flag was propagated to the storage backend when the request was made, and the storage backend is responsible for returning FinishedError when stopped flag is set
    while (true)
    {
        auto r = _reader->async_response();
        if (r.ret == common::ResponseCode::FinishedError)
        {
            LOG(DEBUG) << "FinishedError while waiting for responses";
            throw common::Exception(common::ResponseCode::FinishedError);
        }

        auto & batch = _batches_by_file_index.at(r.file_index);

        if (r.ret != common::ResponseCode::Success)
        {
            _error_by_file_index.at(r.file_index) = r.ret;
        }

        batch.handle_response(r);
    }

    LOG(DEBUG) << "Finished reading files "  << (stopped ? " - terminated" : " successfully");
}

}; // namespace runai::llm::streamer::impl
