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

#include "streamer/impl/cuda/cuda_loader.h"

namespace runai::llm::streamer::impl
{

Batch::Batch(unsigned worker_index, unsigned file_index, const std::string & path, const common::s3::S3ClientWrapper::Params & params, const Tasks && tasks, std::shared_ptr<common::Responder> responder, std::shared_ptr<const Config> config, bool cuda) :
    worker_index(worker_index),
    file_index(file_index),
    path(path),
    object_storage_params(params),
    tasks(tasks),
    range(tasks),
    responder(responder),
    config(config),
    _cuda(cuda)
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
    ASSERT(is_object_storage()) << "S3 params are not initialized";

    request_async_read(reader.get(), stopped);
}

void Batch::execute(std::atomic<bool> & stopped)
{
    LOG(DEBUG) << "Start reading from file " << path;

    auto response_code = common::ResponseCode::Success;
    try
    {
        ASSERT(!is_object_storage()) << "Unsupported reader mode for object storage backends";

        _reader = std::make_unique<File>(path, *config);

        const auto * drv = _cuda ? cuda::CudaDriver::get() : nullptr;
        if (drv != nullptr)
        {
            read_cuda(*config, stopped, *drv);
        }
        else
        {
            if (_cuda)
            {
                LOG(WARNING) << "CUDA driver not available; falling back to pageable memory read for " << path;
            }
            read(*config, stopped);
        }
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
        if (range.size == 0)
        {
            // tensors of size zero are valid, but empty request can be invalid in the storage backend
            LOG(DEBUG) << "Found task of zero size - return response and don't pass to backend";
            handle_task_response(common::ResponseCode::Success, &task);
            continue;
        }
        reader->async_read(object_storage_params, task.info.global_id, range, dst);
    }
}

void Batch::handle_response(const common::backend_api::Response & response, const Task * task_ptr)
{
    // Aborting if a single task failed, we should replace this by a retry mechanism
    if (response.ret != common::ResponseCode::Success)
    {
        LOG(ERROR) << "Error " << response.ret << " while waiting for responses";
        throw common::Exception(response.ret);
    }

    ASSERT(task_ptr != nullptr) << "Received response from a null task";

    handle_task_response(response.ret, task_ptr);
}

void Batch::handle_task_response(const common::ResponseCode response_code, const Task * task_ptr)
{
    // Aborting if a single task failed, we should replace this by a retry mechanism

    ASSERT(task_ptr->request->file_index == file_index) << "Received response from a different file " << task_ptr->request->file_index << " expected " << file_index;

    LOG(SPAM) << "Received object storage response: File index " << file_index << " request index " << task_ptr->request->index << " ret " << response_code;
    if (task_ptr->finished_request(response_code))
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

bool Batch::is_object_storage() const
{
    return object_storage_params.valid();
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

namespace
{

// Thread-local CUDA resources for staging reads from filesystem to device memory.
// One instance exists per worker thread; all batches that run on the same thread
// share the same staging buffer and CUDA stream, avoiding repeated allocations.
struct CudaStagingBuffer
{
    ~CudaStagingBuffer()
    {
        const auto * drv = cuda::CudaDriver::get();
        if (drv == nullptr)
        {
            return;
        }
        if (stream != nullptr)
        {
            drv->cuStreamDestroy(stream);
        }
        if (ptr != nullptr)
        {
            drv->cuMemFreeHost(ptr);
        }
    }

    // Return a pinned host buffer of at least `needed` bytes, (re)allocating if required.
    char * ensure(size_t needed, const cuda::CudaDriver & drv)
    {
        if (stream == nullptr)
        {
            // Worker threads do not automatically inherit the CUDA context from
            // the Python main thread.  Make the already-retained primary context
            // current for this thread before any driver API calls.
            drv.cuCtxSetCurrent(drv.ctx);

            drv.cuStreamCreate(&stream, 0);
        }
        if (needed > capacity)
        {
            if (ptr != nullptr)
            {
                drv.cuMemFreeHost(ptr);
                ptr = nullptr;
            }
            drv.cuMemAllocHost(reinterpret_cast<void **>(&ptr), needed);
            capacity = needed;
        }
        return ptr;
    }

    char * ptr = nullptr;
    size_t capacity = 0;
    ::CUstream stream = nullptr;
};

// One staging buffer per worker thread, shared across all batches on that thread.
thread_local CudaStagingBuffer g_cuda_staging;

} // anonymous namespace

void Batch::read_cuda(const Config & config, std::atomic<bool> & stopped, const cuda::CudaDriver & drv)
{
    if (tasks.empty())
    {
        LOG(DEBUG) << "Empty batch";
        return;
    }

    char * staging_buf = g_cuda_staging.ensure(config.fs_block_bytesize, drv);

    // Single seek: tasks within a batch are contiguous in the file.
    _reader->seek(range.start);

    // Iterate per-task so each tensor lands at its own pre-aligned GPU address.
    // File reads remain sequential; only the GPU write target changes between tasks.
    for (auto & task : tasks)
    {
        if (stopped) break;

        char * gpu_ptr = task.destination();
        size_t remaining = task.info.bytesize;

        while (remaining > 0 && !stopped)
        {
            const size_t to_read = std::min(remaining, config.fs_block_bytesize);
            _reader->read(to_read, staging_buf);
            drv.cuMemcpyHtoDAsync(reinterpret_cast<::CUdeviceptr>(gpu_ptr), staging_buf, to_read, g_cuda_staging.stream);
            drv.cuStreamSynchronize(g_cuda_staging.stream);
            gpu_ptr   += to_read;
            remaining -= to_read;
        }

        finished_until(task.info.end, common::ResponseCode::Success);
    }

    LOG(DEBUG) << "Finished reading " << tasks.size() << " tasks from file " << path
               << " to CUDA device" << (stopped ? " - terminated" : " successfully");

    if (stopped)
    {
        throw common::Exception(common::ResponseCode::FinishedError);
    }
}


}; // namespace runai::llm::streamer::impl
