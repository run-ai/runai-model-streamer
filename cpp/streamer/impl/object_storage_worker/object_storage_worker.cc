#include "streamer/impl/object_storage_worker/object_storage_worker.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "streamer/impl/s3/s3.h"

#include "common/s3_wrapper/s3_wrapper.h"
#include "common/range/range.h"
#include "common/exception/exception.h"

#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

// Start at 1 so a chunk handle is never 0; keeps handle 0 free as an unmistakable "not a real completion"
// value, matching the drained-responder sentinel some plugins emit.
std::atomic<common::backend_api::ObjectRequestId_t> ObjectStorageWorker::_async_handle_counter { 1 };

std::size_t ObjectStorageWorker::capacity(const Workload & first)
{
    // An empty workload (no batches) carries no params to build a client from and nothing to read. The
    // streamer never dispatches empty workloads (they are skipped at dispatch, empty submissions rejected in
    // verify_requests), so this is not a production path. Throw so the base discards it (discard() pushes no
    // responses, since there are no batches) and retries the window on the next workload.
    if (first.batches().empty())
    {
        LOG(WARNING) << "Object storage worker received an empty workload; the streamer is expected to skip these";
        throw common::Exception(common::ResponseCode::EmptyRequestError);
    }

    // Build the persistent reader/client once, from the first workload that can. Keep a shared_ptr to the
    // Config so the reference S3 holds stays valid after the building workload finalizes (batches dropped).
    if (_reader == nullptr)
    {
        const auto & batch = first.batches().begin()->second;
        _config = batch.config;
        _chunk_bytesize = std::max(static_cast<size_t>(1), _config->s3_block_bytesize);

        // Request one completion at a time by default for prompt, per-completion window refill;
        // RUNAI_STREAMER_INTERNAL_MAX_RESPONSES can raise it (internal tuning / test knob).
        _max_responses = static_cast<unsigned>(std::max(1UL, utils::getenv<unsigned long>("RUNAI_STREAMER_INTERNAL_MAX_RESPONSES", 1UL)));

        try
        {
            auto client = std::make_shared<common::s3::S3ClientWrapper>(batch.object_storage_params);
            _reader = std::make_shared<S3>(client, *_config);
        }
        catch (const common::Exception & e)
        {
            // e.g. the plugin library is missing (S3NotSupported). Record the code for discard() and rethrow:
            // the base discards this workload with it and retries the client on the next workload.
            LOG(ERROR) << "Failed to create object storage client: " << e.error();
            _reader_error = e.error();
            throw;
        }
    }

    // The window is a max in-flight chunk count: the plugin's byte window / chunk size (unbounded for
    // gcs/azure, which advertise SIZE_MAX). Each in-flight chunk costs 1.
    const size_t unbounded = static_cast<size_t>(-1);
    const size_t window_bytes = _reader->max_inflight_bytes();
    return (window_bytes == unbounded)
        ? unbounded
        : std::max(static_cast<size_t>(1), window_bytes / _chunk_bytesize);
}

void ObjectStorageWorker::discard(Workload && workload)
{
    // The base could not bring the window up for this workload. Finalize it with a code that reflects why:
    // an empty workload has no batches (so this pushes nothing); a client-build failure uses the recorded
    // code; anything else (e.g. the queue allocation threw) is UnknownError.
    common::ResponseCode code;
    if (workload.batches().empty())
    {
        code = common::ResponseCode::EmptyRequestError;
    }
    else if (_reader_error != common::ResponseCode::Success)
    {
        code = _reader_error;
    }
    else
    {
        code = common::ResponseCode::UnknownError;
    }

    Inflight wl;
    wl.workload = std::move(workload);
    report_workload(wl, code);

    _reader_error = common::ResponseCode::Success;   // reset for the next attempt
}

void ObjectStorageWorker::enqueue(Workload && workload)
{
    // Count chunks up front so we can reserve one contiguous block of handles and size chunk_task_idx.
    size_t total_chunks = 0;
    for (const auto & [file_index, batch] : workload.batches())
    {
        for (const auto & task : batch.tasks)
        {
            if (task.info.bytesize != 0)
            {
                total_chunks += (task.info.bytesize + _chunk_bytesize - 1) / _chunk_bytesize;   // ceil
            }
        }
    }

    // A workload with no chunks (only zero-size tasks) is reported inline and never entered into _inflight:
    // it reserves no handle block, so it would have no unique key, and there is nothing to route to it.
    if (total_chunks == 0)
    {
        Inflight wl;
        wl.workload = std::move(workload);
        // the workload was fully populated via add_batch before dispatch, so batches() is complete here
        for (auto & [file_index, batch] : wl.workload.batches())
        {
            for (const auto & task : batch.tasks)
            {
                common::backend_api::Response resp(common::ResponseCode::Success);
                batch.handle_response(resp, &task);
            }
        }
        report_workload(wl, common::ResponseCode::Success);
        return;
    }

    // enqueue only runs once the window is up (the base creates _queue only after capacity() succeeded), so
    // the reader is built and _chunk_bytesize is correct here - no retry needed.

    // Reserve this workload's contiguous handle block and register it, keyed by the block base.
    const auto handle_base = _async_handle_counter.fetch_add(total_chunks);
    auto [wlit, inserted] = _inflight.emplace(handle_base, Inflight{});
    ASSERT(inserted) << "duplicate handle base " << handle_base;

    Inflight & wl = wlit->second;
    wl.workload = std::move(workload);
    wl.chunk_task_idx.resize(total_chunks);

    size_t next_chunk = 0;
    for (auto & [file_index, batch] : wl.workload.batches())
    {
        for (const auto & task : batch.tasks)
        {
            if (task.info.bytesize == 0)
            {
                // zero-size task: no backend read, complete immediately (handle_response ignores the handle)
                common::backend_api::Response resp(common::ResponseCode::Success);
                batch.handle_response(resp, &task);
                continue;
            }

            const size_t task_idx = wl.tasks.size();
            wl.tasks.push_back(TaskState{ &batch, &task, 0, common::ResponseCode::Success });

            size_t offset = task.info.offset;
            size_t remaining = task.info.bytesize;
            char * buffer = task.destination();
            while (remaining > 0)
            {
                const size_t bs = std::min(remaining, _chunk_bytesize);   // last chunk is the remainder
                wl.chunk_task_idx[next_chunk] = task_idx;
                _queue->enqueue(ObjectChunk{ handle_base + next_chunk, offset, bs, buffer }, 1);   // cost 1
                ++wl.tasks[task_idx].remaining_chunks;
                ++next_chunk;
                offset += bs;
                buffer += bs;
                remaining -= bs;
            }
        }
    }

    wl.remaining_tasks = wl.tasks.size();   // total_chunks > 0 -> at least one non-zero-size task
}

std::pair<ObjectStorageWorker::InflightMap::iterator, size_t> ObjectStorageWorker::locate(common::backend_api::ObjectRequestId_t handle)
{
    // upper_bound gives the first block whose base is > handle; the previous block is the candidate owner.
    auto it = _inflight.upper_bound(handle);
    if (it == _inflight.begin())
    {
        return { _inflight.end(), 0 };   // handle precedes every block
    }
    --it;

    const auto rel = handle - it->first;
    if (rel >= it->second.chunk_task_idx.size())
    {
        return { _inflight.end(), 0 };   // falls in a gap between blocks / past this block
    }
    return { it, it->second.chunk_task_idx[rel] };
}

void ObjectStorageWorker::submit(const ObjectChunk & chunk)
{
    auto [wlit, task_idx] = locate(chunk.handle);
    ASSERT(wlit != _inflight.end()) << "submitting a chunk with unknown handle " << chunk.handle;

    TaskState & ts = wlit->second.tasks[task_idx];

    // the owning task has already failed a chunk: don't waste a backend read on a doomed task; account for
    // this chunk now. Chunks already issued before the failure still land and complete via drain_batch.
    if (ts.error != common::ResponseCode::Success)
    {
        complete_chunk(wlit, task_idx, ts.error);
        return;
    }

    try
    {
        const common::Range range(chunk.offset, chunk.bytesize);
        _reader->async_read(ts.batch->object_storage_params, chunk.handle, range, chunk.buffer);
    }
    catch (const common::Exception & e)
    {
        // the read could not be issued: fail this chunk (marks the task, short-circuits its siblings)
        complete_chunk(wlit, task_idx, e.error());
    }
    catch (...)
    {
        complete_chunk(wlit, task_idx, common::ResponseCode::UnknownError);
    }
}

void ObjectStorageWorker::complete_chunk(InflightMap::iterator wlit, size_t task_idx, common::ResponseCode ret)
{
    _queue->complete(1);   // free the window slot so the next chunk can be submitted

    Inflight & wl = wlit->second;
    TaskState & ts = wl.tasks[task_idx];

    ASSERT(ts.remaining_chunks > 0) << "chunk completion for a task with no remaining chunks";

    if (ret != common::ResponseCode::Success && ts.error == common::ResponseCode::Success)
    {
        ts.error = ret;   // first failing chunk wins for this task
    }

    if (--ts.remaining_chunks == 0)
    {
        // once every chunk of the task has completed, report its aggregate result: success goes through
        // handle_response; a failure is recorded per file and the task is left for report_workload to fail
        // (handle_response throws on non-success)
        if (ts.error == common::ResponseCode::Success)
        {
            common::backend_api::Response resp(common::ResponseCode::Success);
            ts.batch->handle_response(resp, ts.task);
        }
        else
        {
            wl.error_by_file_index.emplace(ts.batch->file_index, ts.error);   // first error per file
        }

        if (--wl.remaining_tasks == 0)
        {
            finalize(wlit, common::ResponseCode::Success);
        }
    }
}

void ObjectStorageWorker::report_workload(Inflight & wl, common::ResponseCode code)
{
    for (auto & [file_index, batch] : wl.workload.batches())
    {
        // whole-workload abort fails every file; otherwise fail only files with a recorded error
        // (handle_error(Success) is a no-op for files whose tasks all completed)
        auto error_code = code;
        if (error_code == common::ResponseCode::Success)
        {
            auto it = wl.error_by_file_index.find(file_index);
            if (it != wl.error_by_file_index.end())
            {
                error_code = it->second;
            }
        }
        batch.handle_error(error_code);
    }
}

void ObjectStorageWorker::finalize(InflightMap::iterator wlit, common::ResponseCode code)
{
    report_workload(wlit->second, code);
    _inflight.erase(wlit);
}

void ObjectStorageWorker::abort_all(common::ResponseCode code)
{
    for (auto it = _inflight.begin(); it != _inflight.end(); )
    {
        auto next = std::next(it);
        finalize(it, code);   // fails every batch and erases `it`
        it = next;
    }

    // zero the window so idle() becomes true and the pool can join: move any pending chunk into flight,
    // then release every in-flight slot (all costs are 1).
    if (_queue != nullptr)
    {
        while (_queue->try_take()) {}
        const auto n = _queue->inflight();
        for (size_t i = 0; i < n; ++i)
        {
            _queue->complete(1);
        }
    }
}

void ObjectStorageWorker::drain_batch(std::atomic<bool> & stopped)
{
    if (_reader == nullptr)
    {
        return;   // nothing was ever submitted
    }

    if (stopped)
    {
        abort_all(common::ResponseCode::FinishedError);   // teardown: fail all in flight, empty the window
        return;
    }

    std::vector<common::backend_api::Response> responses;
    const auto r = _reader->async_response(responses, _max_responses);
    if (r != common::ResponseCode::Success)
    {
        // FinishedError = responder stopped/drained; any other code is a backend failure. Either way, no
        // more completions are coming for this worker's in-flight chunks - fail them all.
        if (r != common::ResponseCode::FinishedError)
        {
            LOG(ERROR) << "Object storage responder returned " << r;
        }
        abort_all(r);
        return;
    }

    bool progressed = false;
    for (const auto & response : responses)
    {
        // Some plugins (azure/gcs) append an "empty" FinishedError event once the responder runs dry, to
        // signal there is nothing more to hand out this round. It is not a real completion - stop here.
        if (response.ret == common::ResponseCode::FinishedError)
        {
            break;
        }

        auto [wlit, task_idx] = locate(response.handle);
        if (wlit == _inflight.end())
        {
            // a handle outside every in-flight block: a stale/cancelled completion or a buggy backend.
            // Guard here (ASSERT is stripped in release) to avoid mis-routing, and fail the worker's work.
            LOG(ERROR) << "Received response with unknown handle " << response.handle;
            abort_all(common::ResponseCode::UnknownError);
            return;
        }

        complete_chunk(wlit, task_idx, response.ret);
        progressed = true;
    }

    // Blocking async_response returned without a real completion while chunks are still in flight: the
    // responder was stopped or drained early. Abort rather than spin re-reading the same sentinel.
    if (!progressed && _queue != nullptr && !_queue->idle())
    {
        abort_all(common::ResponseCode::FinishedError);
    }
}

}; // namespace runai::llm::streamer::impl
