
#include "streamer/impl/workload/workload.h"

#include <memory>
#include <utility>

#include "streamer/impl/s3/s3.h"

#include "common/response_code/response_code.h"
#include "common/exception/exception.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

std::atomic<common::backend_api::ObjectRequestId_t> Workload::_async_handle_counter {0};

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
        _is_object_storage = batch.is_object_storage();
    }
    else if  (auto res = verify_batch(batch); res != common::ResponseCode::Success)
    {
        return res;
    }

    _total_tasks += batch.tasks.size();
    _batches_by_file_index.emplace(file_index, std::move(batch));

    return common::ResponseCode::Success;
}

bool Workload::is_object_storage() const
{
    return _is_object_storage;
}

common::ResponseCode Workload::verify_batch(const Batch & batch)
{
    if (batch.is_object_storage() != is_object_storage())
    {
         LOG(ERROR) << "Workload contains paths of different storage backends";

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
    if (is_object_storage())
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

void Workload::assign_global_ids()
{
    _global_id_base = _async_handle_counter.fetch_add(_total_tasks);
    LOG(DEBUG) << "Assigned global ids for " << _total_tasks << " tasks starting from " << _global_id_base;
    _tasks.resize(_total_tasks);
    size_t counter = 0;
    auto base_id = _global_id_base;

    for (auto & [file_index, batch] : _batches_by_file_index)
    {
        for (const auto & task : batch.tasks)
        {
            task.info.global_id = base_id++;
            _tasks[counter++] = &task;
        }
    }
}

void Workload::async_read(std::atomic<bool> & stopped)
{
    auto response_code = common::ResponseCode::Success;
    try
    {
        assign_global_ids();

        const auto & config = _batches_by_file_index.begin()->second.config;

        auto s3_client = std::make_shared<common::s3::S3ClientWrapper>(_batches_by_file_index.begin()->second.object_storage_params);
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
        std::vector<common::backend_api::Response> responses;
        auto r = _reader->async_response(responses, 1);
        if (r == common::ResponseCode::FinishedError)
        {
            LOG(DEBUG) << "FinishedError while waiting for responses";
            throw common::Exception(common::ResponseCode::FinishedError);
        }

        const auto & response = responses.back();

        if (response.ret == common::ResponseCode::FinishedError)
        {
            LOG(DEBUG) << "FinishedError while waiting for responses";
            throw common::Exception(common::ResponseCode::FinishedError);
        }

        ASSERT(response.handle >= _global_id_base) << "Received response with invalid handle " << response.handle << " expected at least " << _global_id_base;

        auto index = response.handle - _global_id_base;
        ASSERT(index < _tasks.size()) << "Received response with invalid handle " << response.handle << " expected at most " << _global_id_base + _tasks.size();

        const Task * task_ptr = _tasks[index];
        ASSERT(task_ptr != nullptr) << "Received response from a null task ; response: " << response;

        auto file_index = task_ptr->request->file_index;
        auto & batch = _batches_by_file_index.at(file_index);

        if (response.ret != common::ResponseCode::Success)
        {
            _error_by_file_index.at(file_index) = response.ret;
        }

        batch.handle_response(response, task_ptr);
    }

    LOG(DEBUG) << "Finished reading files "  << (stopped ? " - terminated" : " successfully");
}

}; // namespace runai::llm::streamer::impl
