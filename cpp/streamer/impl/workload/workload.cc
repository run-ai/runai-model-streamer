
#include "streamer/impl/workload/workload.h"

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "streamer/impl/s3/s3.h"

#include "common/range/range.h"
#include "common/response_code/response_code.h"
#include "common/exception/exception.h"
#include "common/backend_api/response/response.h"

#include "utils/capacity_queue/capacity_queue.h"
#include "utils/env/env.h"
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

namespace
{

// A single chunk of a task's range, submitted as one ranged read. The owning task is not
// stored here: it is recovered from request_id via chunk_task_idx (the same map the
// completion path uses), keeping a single source of truth for the chunk -> task mapping.
struct ObjectChunk
{
    common::backend_api::ObjectRequestId_t request_id;   // unique per chunk
    size_t offset;                                       // absolute file offset
    size_t bytesize;                                     // chunk length
    char * buffer;                                       // destination
};

} // namespace

void Workload::async_read(std::atomic<bool> & stopped)
{
    auto response_code = common::ResponseCode::Success;

    // Per-file error for isolated reporting: a task that fails fails only its own file's
    // batch; other files in the same workload still complete successfully. Populated while
    // draining and consumed by handle_error() below. A whole-workload abort (stop /
    // FinishedError / unexpected exception) sets response_code instead and fails every file.
    std::map<unsigned, common::ResponseCode> error_by_file_index;

    try
    {
        assign_global_ids();

        const auto & config = _batches_by_file_index.begin()->second.config;

        auto s3_client = std::make_shared<common::s3::S3ClientWrapper>(_batches_by_file_index.begin()->second.object_storage_params);
        _reader = std::make_shared<S3>(s3_client, *config);

        // Worker-owned scheduling (single thread): split each task's range into chunks and
        // submit them up to the backend's in-flight window, refilling as completions free
        // credit. We count chunks: the window is a max in-flight chunk count (the plugin's
        // byte window / chunk size), each in-flight chunk costs 1, and a task is complete
        // when its expected number of chunks has finished. Only this thread touches the
        // queue and the per-task counters, so no locking / atomics are needed.
        const size_t chunk_bytesize = std::max(static_cast<size_t>(1), config->s3_block_bytesize);
        const size_t unbounded = static_cast<size_t>(-1);
        const size_t window_bytes = _reader->max_inflight_bytes();
        const size_t window_chunks = (window_bytes == unbounded)
            ? unbounded
            : std::max(static_cast<size_t>(1), window_bytes / chunk_bytesize);

        utils::CapacityQueue<ObjectChunk> queue(window_chunks);

        // per-task bookkeeping, indexed by task index (global_id - base); touched only by
        // this thread. task_error holds the first failing chunk's code for the task (Success
        // while all its chunks have succeeded). task_batch is the batch that owns the task,
        // so completion handling never has to look a task up by request->file_index.
        std::vector<unsigned> task_remaining_chunks(_total_tasks, 0);
        std::vector<Batch *> task_batch(_total_tasks, nullptr);
        std::vector<common::ResponseCode> task_error(_total_tasks, common::ResponseCode::Success);

        // count chunks up front so we can reserve one contiguous block of request ids and map
        // each id back to its task with a flat vector (id - chunk_id_base) instead of a hash map
        size_t total_chunks = 0;
        for (const auto & [file_index, batch] : _batches_by_file_index)
        {
            for (const auto & task : batch.tasks)
            {
                if (task.info.bytesize != 0)
                {
                    total_chunks += (task.info.bytesize + chunk_bytesize - 1) / chunk_bytesize;   // ceil
                }
            }
        }

        // Reserve one contiguous block of request ids for this workload's chunks. assign_global_ids
        // already advanced the counter past this workload's task ids, so when total_chunks > 0 the
        // counter is >= 1 here and chunk_id_base >= 1: request id 0 is never a valid chunk id. That
        // keeps handle 0 reserved for the drained-responder FinishedError sentinel (see the drain
        // loop below), so a sentinel can never be mistaken for a real completion.
        const auto chunk_id_base = _async_handle_counter.fetch_add(total_chunks);
        std::vector<size_t> chunk_task_idx(total_chunks);   // request id (- base) -> owning task index

        size_t next_chunk = 0;
        for (auto & [file_index, batch] : _batches_by_file_index)
        {
            for (const auto & task : batch.tasks)
            {
                if (task.info.bytesize == 0)
                {
                    // zero-size task: no backend request, complete immediately
                    common::backend_api::Response resp(task.info.global_id, common::ResponseCode::Success);
                    batch.handle_response(resp, &task);
                    continue;
                }

                const size_t idx = task.info.global_id - _global_id_base;
                task_batch[idx] = &batch;
                size_t offset = task.info.offset;
                size_t remaining = task.info.bytesize;
                char * buffer = task.destination();
                while (remaining > 0)
                {
                    const size_t bs = std::min(remaining, chunk_bytesize);   // last chunk is the remainder
                    // each chunk carries its own unique request id (the backend requires unique
                    // ids per in-flight request); the flat vector maps it back to the owning task
                    const auto chunk_id = chunk_id_base + next_chunk;
                    chunk_task_idx[next_chunk] = idx;
                    queue.enqueue(ObjectChunk{chunk_id, offset, bs, buffer}, 1);   // cost 1: count chunks
                    offset += bs;
                    buffer += bs;
                    remaining -= bs;
                    ++task_remaining_chunks[idx];
                    ++next_chunk;
                }
            }
        }

        // Account for one completed chunk of task idx, whether it came back from the backend or
        // was short-circuited without a read. Frees the window slot, records the task's first
        // error, and once all of the task's chunks are done reports its aggregate result. Called
        // only from this worker thread (the drain loop and submit's short-circuit), so the two
        // completion paths share a single authority and cannot drift.
        size_t completed_chunks = 0;
        auto complete_chunk = [&](size_t idx, common::ResponseCode ret)
        {
            ASSERT(_tasks[idx] != nullptr) << "Completing a chunk of a null task at index " << idx;
            ASSERT(task_remaining_chunks[idx] > 0) << "Chunk completion for task " << _tasks[idx]->info.global_id << " with no remaining chunks";

            queue.complete(1);   // free the window slot so the next chunk can be submitted

            if (ret != common::ResponseCode::Success && task_error[idx] == common::ResponseCode::Success)
            {
                task_error[idx] = ret;   // first failing chunk wins for this task
            }

            // once every chunk of the task has completed, report its aggregate result: success
            // goes through handle_response; a failure is recorded per file and the task is left
            // unfinished so handle_error() below fails it (handle_response throws on non-success)
            if (--task_remaining_chunks[idx] == 0)
            {
                if (task_error[idx] == common::ResponseCode::Success)
                {
                    common::backend_api::Response task_resp(_tasks[idx]->info.global_id, common::ResponseCode::Success);
                    task_batch[idx]->handle_response(task_resp, _tasks[idx]);
                }
                else
                {
                    error_by_file_index.emplace(task_batch[idx]->file_index, task_error[idx]);   // first error per file
                }
            }

            ++completed_chunks;
        };

        auto submit = [&](const ObjectChunk & c)
        {
            // request ids are the contiguous block [chunk_id_base, chunk_id_base + total_chunks)
            ASSERT(c.request_id >= chunk_id_base && c.request_id - chunk_id_base < total_chunks)
                << "Chunk request id " << c.request_id << " outside [" << chunk_id_base << ", " << (chunk_id_base + total_chunks) << ")";
            const size_t idx = chunk_task_idx[c.request_id - chunk_id_base];   // owning task

            // the owning task has already failed a chunk: don't waste a backend read on a doomed
            // task. account for this chunk as completed now (with the task's error). chunks that
            // were already issued before the failure still land and are handled by the drain loop.
            if (task_error[idx] != common::ResponseCode::Success)
            {
                complete_chunk(idx, task_error[idx]);
                return;
            }

            const common::Range range(c.offset, c.bytesize);
            _reader->async_read(task_batch[idx]->object_storage_params, c.request_id, range, c.buffer);
        };

        auto pump = [&]()
        {
            while (auto c = queue.try_take())
            {
                submit(*c);
            }
        };

        // prime the window, then refill as completions free credit
        pump();

        // Request one completion at a time by default for prompt, per-completion window refill.
        // The drain loop handles a batch of several responses correctly (and tolerates the
        // drained-responder sentinel), so RUNAI_STREAMER_INTERNAL_MAX_RESPONSES can raise this to
        // reduce wait() calls at the cost of refill granularity (internal tuning / test knob).
        const unsigned max_responses = static_cast<unsigned>(std::max(1UL, utils::getenv<unsigned long>("RUNAI_STREAMER_INTERNAL_MAX_RESPONSES", 1UL)));

        while (completed_chunks < total_chunks)
        {
            if (stopped)
            {
                throw common::Exception(common::ResponseCode::FinishedError);
            }

            std::vector<common::backend_api::Response> responses;
            auto r = _reader->async_response(responses, max_responses);
            if (r == common::ResponseCode::FinishedError)
            {
                throw common::Exception(common::ResponseCode::FinishedError);
            }

            const size_t completed_before = completed_chunks;
            for (const auto & response : responses)
            {
                // Some plugins (azure/gcs) drain their per-client responder up to max_responses
                // and append an "empty" FinishedError event (handle 0) once it runs dry, to
                // signal there is nothing more to hand out this round. It is not a real chunk
                // completion - stop consuming the batch here.
                if (response.ret == common::ResponseCode::FinishedError)
                {
                    break;
                }

                // Hard range check before the (unsigned) subtraction: a stale/cancelled
                // completion or a buggy backend could deliver a handle outside this workload's
                // reserved block. Guarding here (not just via ASSERT, which is stripped in
                // release builds) prevents an out-of-bounds read of chunk_task_idx.
                if (response.handle < chunk_id_base || response.handle - chunk_id_base >= total_chunks)
                {
                    LOG(ERROR) << "Received response with out-of-range handle " << response.handle;
                    throw common::Exception(common::ResponseCode::UnknownError);
                }
                const size_t rel = response.handle - chunk_id_base;
                complete_chunk(chunk_task_idx[rel], response.ret);
            }

            // refill the window; this can also short-circuit doomed chunks, which advances
            // completed_chunks without a backend read
            pump();

            // no completion of any kind this round (only the drained/stopped sentinel) while
            // chunks remain means the responder was stopped or drained early: abort rather than
            // spin re-reading the same sentinel
            if (completed_chunks == completed_before && completed_chunks < total_chunks)
            {
                throw common::Exception(common::ResponseCode::FinishedError);
            }
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
        // whole-workload abort fails every file; otherwise fail only files with a recorded
        // error (handle_error(Success) is a no-op for files whose tasks all completed)
        auto error_code = response_code;
        if (error_code == common::ResponseCode::Success)
        {
            auto it = error_by_file_index.find(file_index);
            if (it != error_by_file_index.end())
            {
                error_code = it->second;
            }
        }
        batch.handle_error(error_code);
    }
}

}; // namespace runai::llm::streamer::impl
