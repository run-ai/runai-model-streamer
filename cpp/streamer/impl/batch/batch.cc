#include "streamer/impl/batch/batch.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <set>
#include <map>
#include <vector>

#include "utils/logging/logging.h"

#include "common/exception/exception.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/range/range.h"
#include "streamer/impl/reader/reader.h"
#include "streamer/impl/file/file.h"
#include "streamer/impl/s3/s3.h"

namespace runai::llm::streamer::impl
{

Batch::Batch(unsigned worker_index, unsigned file_index, const std::string & path, const common::s3::S3ClientWrapper::Params & params, const Tasks && tasks, std::shared_ptr<common::Responder> responder, std::shared_ptr<const Config> config) :
    worker_index(worker_index),
    file_index(file_index),
    path(path),
    params(params),
    tasks(tasks),
    range(tasks),
    responder(responder),
    config(config)
{
    LOG(DEBUG) << "Batch " << path << " range " << range << " ; " << this->tasks.size() << " tasks";
}

size_t Batch::total_bytes() const
{
    return range.size;
}

size_t Batch::end_offset() const
{
    return range.end;
}

void Batch::request(std::shared_ptr<Reader> reader, std::atomic<bool> & stopped)
{
    ASSERT(reader != nullptr) << "Reader is not initialized";
    ASSERT(params.valid()) << "S3 params are not initialized";

    request_async_read(reader.get(), stopped);
}

void Batch::execute(std::atomic<bool> & stopped)
{
    LOG(DEBUG) << "Start reading from file " << path;

    auto response_code = common::ResponseCode::Success;
    try
    {
        ASSERT(!params.valid()) << "Unsupported reader mode for object storage backends";

        _reader = std::make_unique<File>(path, *config);
        read(*config, stopped);
    }
    catch(const common::Exception & e)
    {
        response_code = e.error();
    }
    catch (...)
    {
        response_code = common::ResponseCode::UnknownError;
    }

    // in case of an error all of the batch's unfinished tasks are failed with the same error code
    // in case of success the finished tasks were already notified
    handle_error(response_code);
}

void Batch::handle_error(common::ResponseCode response_code)
{
    // in case of an error all of the batch's unfinished tasks are failed with the same error code
    // in case of success the finished tasks were already notified

    if (response_code != common::ResponseCode::Success)
    {
        if (response_code != common::ResponseCode::FinishedError)
        {
            LOG(ERROR) << "Failed to read from file " << path << " ; error: " << response_code;
        }
        else
        {
            LOG(SPAM) << "Finished reading from file " << path;
        }

        // Note:
        // At this point no more tasks are expected to finish, since synchronous reading has ended and for asyncronous reading the thread stopped waiting for finished tasks
        for (auto & task : tasks)
        {
            if (task.finished_request(response_code))
            {
                common::Response response(file_index, task.request->index, task.request->ret());
                responder->push(std::move(response), task.request->bytesize);
            }
        }
    }
}

// read the entire range and send notifications for each sub range
void Batch::read(const Config & config, std::atomic<bool> & stopped)
{
    if (tasks.empty())
    {
        LOG(DEBUG) << "Empty batch";
        return;
    }

    auto file_offset = range.start;
    // For CPU buffer we assume that all the requests are written to a single continous buffer
    char * buffer = tasks[0].destination();;

    size_t num_chunks = range.size / config.fs_block_bytesize;

    // seek just once because tasks are consecutive within the range
    _reader->seek(file_offset);

    // read task's range in chunks
    size_t i = 0;
    for (; i < num_chunks && !stopped; ++i)
    {
        _reader->read(config.fs_block_bytesize, buffer);

        file_offset += config.fs_block_bytesize;
        buffer += config.fs_block_bytesize;

        finished_until(file_offset, common::ResponseCode::Success);
    }

    if (file_offset < range.end && !stopped)
    {
        num_chunks++;
        i = 1;
        _reader->read(range.end - file_offset, buffer);
        finished_until(range.end, common::ResponseCode::Success);
    }

    LOG(DEBUG) << "Finished reading " << i << "/" << num_chunks << " chunks from file " << path << (stopped ? " - terminated" : " successfully");

    if (stopped)
    {
        throw common::Exception(common::ResponseCode::FinishedError);
    }
}

void Batch::request_async_read(Reader * reader, std::atomic<bool> & stopped)
{
    if (stopped)
    {
        throw common::Exception(common::ResponseCode::FinishedError);
    }

    // For CPU buffer we assume that all the requests are written to a single continous buffer

    // request asynchronous read for each task
    for (auto & task : tasks)
    {
        auto dst = task.destination();
        common::Range range(task.info.offset, task.info.bytesize);
        common::backend_api::ObjectRequestId_t request_handle = reinterpret_cast<common::backend_api::ObjectRequestId_t>(&task);
        reader->async_read(params, request_handle, range, dst);
    }
}

void Batch::handle_response(const common::backend_api::Response & response)
{
    // Aborting if a single task failed, we should replace this by a retry mechanism
    if (response.ret != common::ResponseCode::Success)
    {
        LOG(ERROR) << "Error " << response.ret << " while waiting for responses";
        throw common::Exception(response.ret);
    }

    // TO do (Noa)
    // safer approach is to use a map of request_id to task index, and verify the pointer is valid
    // also replace request_id with handle
    Task * task_ptr = reinterpret_cast<Task *>(response.handle);
    ASSERT(task_ptr != nullptr) << "Received response from a null task";

    ASSERT(task_ptr->request->file_index == file_index) << "Received response from a different file " << task_ptr->request->file_index << " expected " << file_index;

    LOG(SPAM) << "Received object storage response: File index " << file_index << " request index " << task_ptr->request->index << " ret " << response.ret;
    if (task_ptr->finished_request(response.ret))
    {
        common::Response request_response(file_index, task_ptr->request->index, task_ptr->request->ret());
        responder->push(std::move(request_response), task_ptr->request->bytesize);
    }
}

// notify unfinished tasks up to but not including offset end
void Batch::finished_until(size_t file_offset, common::ResponseCode ret /*= common::ResponseCode::Success */)
{
    unsigned i = _unfinished;
    for (; i < tasks.size(); ++i)
    {
        if (file_offset < tasks[i].info.end)
        {
            break;
        }
        if (tasks[i].finished_request(ret))
        {
            const auto & r = tasks[i].request;
            common::Response response(file_index, r->index, r->ret());
            LOG(SPAM) << "Sending response " << response;
            responder->push(std::move(response), tasks[i].request->bytesize);
        }
    }
    _unfinished = i;
}

unsigned Batch::finished_until() const
{
    return _unfinished;
}

std::ostream & operator<<(std::ostream & os, const Batch & r)
{
    return os << r.path << " range " << r.range << " ; " << r.tasks.size() << " tasks";
}

Batch::Range::Range(size_t start_offset, size_t end_offset) :
    common::Range(start_offset, end_offset - start_offset),
    end(end_offset)
{
    if (end < start)
    {
        LOG(ERROR) << "Invalid range " << start << " - " << end;
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }
}

Batch::Range::Range(const Tasks & tasks) :
    Range(calculate_start(tasks), calculate_end(tasks))
{}

size_t Batch::Range::calculate_start(const Tasks & tasks)
{
    if (tasks.empty())
    {
        return 0;
    }
    return tasks[0].info.offset;
}

size_t Batch::Range::calculate_end(const Tasks & tasks)
{
    if (tasks.empty())
    {
        return 0;
    }

    return tasks[tasks.size() - 1].info.end;
}

std::ostream & operator<<(std::ostream & os, const Batch::Range & r)
{
    return os << "Range from " << r.start << " to " << r.end;
}


}; // namespace runai::llm::streamer::impl
