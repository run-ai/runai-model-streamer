#include "streamer/impl/object_storage_worker/object_storage_worker.h"

#include <algorithm>
#include <chrono>
#include <thread>
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

ObjectStorageWorker::ObjectStorageWorker(std::function<common::s3::Credentials()> credentials_provider) :
    _credentials_provider(std::move(credentials_provider))
{}

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
        const auto & batch = first.batches().front();
        _config = batch.config;
        _chunk_bytesize = std::max(static_cast<size_t>(1), _config->s3_block_bytesize);
        _retry = ObjectStorageRetry(_config->object_storage_retry_timeout);

        // Request one completion at a time by default for prompt, per-completion window refill;
        // RUNAI_STREAMER_INTERNAL_MAX_RESPONSES can raise it (internal tuning / test knob).
        _max_responses = static_cast<unsigned>(std::max(1UL, utils::getenv<unsigned long>("RUNAI_STREAMER_INTERNAL_MAX_RESPONSES", 1UL)));

        try
        {
            // Credentials are streamer-scoped and read exactly once, here at client creation (never on the
            // per-request path). The batch params carry only the URI; combine it with the credentials for the
            // client config.
            const auto credentials = _credentials_provider();
            common::s3::S3ClientWrapper::Params client_params(batch.object_storage_params.uri, credentials, _chunk_bytesize);
            auto client = std::make_shared<common::s3::S3ClientWrapper>(client_params);
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
    for (const auto & batch : workload.batches())
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
        for (auto & batch : wl.workload.batches())
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

    // Reserve this workload's contiguous handle block.
    const auto handle_base = _async_handle_counter;
    _async_handle_counter += total_chunks;

    // Registration and chunk-building allocate (the map node, chunk_task_idx, the tasks vector, the queue
    // entries); under memory pressure any of these can throw. The workload's expected responses were already
    // counted (responder increment + submissions add) before dispatch, so bailing out here without pushing
    // them hangs the consumer forever. On a throw we finalize the workload as UnknownError instead - best
    // effort (report_workload could itself fail under severe OOM).
    try
    {
        auto [wlit, inserted] = _inflight.emplace(handle_base, Inflight{});
        ASSERT(inserted) << "duplicate handle base " << handle_base;

        Inflight & wl = wlit->second;
        wl.workload = std::move(workload);   // noexcept; the workload now lives in the _inflight entry
        wl.chunks.resize(total_chunks);

        size_t next_chunk = 0;
        // &batch below outlives this loop (it is stored in wl.tasks and used to route completions): the
        // workload has already been moved into its _inflight entry, _inflight is node-stable, and no batch is
        // added to a dispatched workload - so the batches vector is never grown or moved again.
        for (auto & batch : wl.workload.batches())
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
                    ObjectChunk chunk{ handle_base + next_chunk, offset, bs, buffer };
                    wl.chunks[next_chunk] = ChunkState{ chunk, task_idx };
                    _queue->enqueue(chunk, 1);   // cost 1
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
    catch (...)
    {
        // OOM mid-registration. The caller aborts on any UnknownError, so rather than reconstruct exact
        // responses we fail this worker's in-flight workloads (this one included - the bulk allocations run
        // after the move, so it is already in _inflight) and zero the window, clearing the half-built entry
        // and any chunks enqueued before the throw (else a stale one later hits submit()'s unknown-handle ASSERT).
        abort_all(common::ResponseCode::UnknownError);
    }
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
    if (rel >= it->second.chunks.size())
    {
        return { _inflight.end(), 0 };   // falls in a gap between blocks / past this block
    }
    return { it, static_cast<size_t>(rel) };
}

void ObjectStorageWorker::submit(const ObjectChunk & chunk)
{
    auto [wlit, chunk_idx] = locate(chunk.handle);
    ASSERT(wlit != _inflight.end()) << "submitting a chunk with unknown handle " << chunk.handle;

    ChunkState & cs = wlit->second.chunks[chunk_idx];
    const size_t task_idx = cs.task_idx;
    TaskState & ts = wlit->second.tasks[task_idx];

    // the owning task has already failed a chunk: don't waste a backend read on a doomed task; account for
    // this chunk now. Chunks already issued before the failure still land and complete via drain_batch.
    if (ts.error != common::ResponseCode::Success)
    {
        complete_chunk(wlit, chunk_idx, ts.error);
        return;
    }

    // ObjectStorageRetry starts the deadline here, so queueing before the first backend attempt does not
    // consume the chunk's retry budget. A delayed retry promoted after its deadline is rejected here.
    if (_retry.enabled() && !_retry.begin_attempt(cs.retry))
    {
        LOG(WARNING) << "Object chunk " << chunk.handle << " exhausted RUNAI_STREAMER_S3_TIMEOUT after "
                     << _retry.retry_count(cs.retry) << " application retries";
        complete_chunk(wlit, chunk_idx, common::ResponseCode::FileAccessError);
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
        complete_chunk(wlit, chunk_idx, e.error());
    }
    catch (...)
    {
        complete_chunk(wlit, chunk_idx, common::ResponseCode::UnknownError);
    }
}

void ObjectStorageWorker::complete_chunk(InflightMap::iterator wlit, size_t chunk_idx, common::ResponseCode ret)
{
    _queue->complete(1);   // free the window slot so the next chunk can be submitted

    Inflight & wl = wlit->second;
    const size_t task_idx = wl.chunks[chunk_idx].task_idx;
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

void ObjectStorageWorker::promote_due_retries()
{
    const auto now = ObjectStorageRetry::Clock::now();
    while (const auto handle = _retry.pop_due(now))
    {
        auto [wlit, chunk_idx] = locate(handle.value());
        ASSERT(wlit != _inflight.end()) << "retrying a chunk with unknown handle " << handle.value();
        _queue->enqueue_front(wlit->second.chunks[chunk_idx].chunk, 1);
    }
}

void ObjectStorageWorker::pre_pump()
{
    if (_retry.enabled())
    {
        promote_due_retries();
    }
}

bool ObjectStorageWorker::has_deferred_work() const
{
    return _retry.has_pending();
}

void ObjectStorageWorker::report_workload(Inflight & wl, common::ResponseCode code)
{
    for (auto & batch : wl.workload.batches())
    {
        // whole-workload abort fails every file; otherwise fail only files with a recorded error
        // (handle_error(Success) is a no-op for files whose tasks all completed)
        auto error_code = code;
        if (error_code == common::ResponseCode::Success)
        {
            // batch.file_index, not the batch's position: one file can contribute several batches (one per
            // contiguous transfer), and errors are recorded per file, so several batches can share an entry
            auto it = wl.error_by_file_index.find(batch.file_index);
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
    // Fail and drop every in-flight workload - including any whose reads are still outstanding at the
    // backend. Those reads are not cancelled: they can still finish and be delivered later as "late
    // completions" - completion events whose handle points into a handle block we erase here. drain_batch
    // recognises a late completion (locate() returns end()) and simply drops it, so abandoning the reads
    // now is safe. This holds even when the worker keeps running afterwards: most calls are on a terminal
    // client state (teardown, or the responder stopped / drained early), but enqueue also calls abort_all
    // mid-life on an allocation failure (OOM). Late completions stay safe either way because handle blocks
    // are allocated from a monotonic counter and never reused: every erased block sits strictly below any
    // future block, so a late completion from an erased block has a handle below every live block's base and
    // always locates to end(). The cost of the mid-life call is that sibling in-flight workloads on this
    // worker are failed too, and their still-outstanding reads may write to already-reported buffers - the
    // OOM caller is expected to abort on UnknownError and tear the streamer down.
    _retry.clear();

    for (auto it = _inflight.begin(); it != _inflight.end(); )
    {
        auto next = std::next(it);
        finalize(it, code);   // fails every batch and erases `it`
        it = next;
    }

    // Zero the window so idle() becomes true and the pool can join. clear() drops every pending chunk and
    // releases all in-flight credit in one step - the workloads those chunks belonged to were already failed
    // and erased above, so their tracking is gone. A try_take()/complete() drain could not do this: try_take()
    // stops at the full-window boundary, so a workload with chunks >> capacity would leave the remainder in
    // _pending (idle() never true from a single call, and the outer loop would re-pump spurious submit()s).
    if (_queue != nullptr)
    {
        _queue->clear();
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

    // With no backend attempt in flight, sleep in short slices while a retry is deferred so the thread does
    // not busy-wait. CapacityWorker calls pump() immediately after this returns; pre_pump() then promotes a
    // due retry to the front of the queue before any newly queued chunks are selected.
    if (_queue->inflight() == 0)
    {
        if (_retry.enabled() && _retry.has_pending() && _queue->empty())
        {
            if (const auto retry_at = _retry.next_due())
            {
                const auto now = ObjectStorageRetry::Clock::now();
                const auto until = std::min(retry_at.value(), now + std::chrono::milliseconds(20));
                std::this_thread::sleep_until(until);
            }
        }
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
    bool responder_drained = responses.empty();   // Success but no events -> responder ran dry this round
    for (const auto & response : responses)
    {
        // Some plugins (azure/gcs) append an "empty" FinishedError event once the responder runs dry, to
        // signal there is nothing more to hand out this round. It is not a real completion - stop here.
        if (response.ret == common::ResponseCode::FinishedError)
        {
            responder_drained = true;
            break;
        }

        auto [wlit, chunk_idx] = locate(response.handle);
        if (wlit == _inflight.end())
        {
            // A late completion (see abort_all): a chunk whose workload was already erased by an abort_all
            // while its read was still outstanding at the backend, now delivered - its handle falls in an
            // erased block. Nothing is left to complete for it, so drop it and keep scanning. Never
            // abort_all here: that would fail every OTHER in-flight submission over a stray event. (A
            // corrupt / never-issued handle from a buggy plugin lands here too and is likewise dropped.)
            LOG(DEBUG) << "Dropping late object storage completion with unknown handle " << response.handle;
            continue;
        }

        auto ret = response.ret;
        if (ret == common::ResponseCode::RetryableFileAccessError)
        {
            ChunkState & cs = wlit->second.chunks[chunk_idx];
            TaskState & ts = wlit->second.tasks[cs.task_idx];
            const auto retry = ts.error == common::ResponseCode::Success
                ? _retry.schedule(cs.retry, cs.chunk.handle)
                : std::nullopt;
            if (retry.has_value())
            {
                // The failed attempt is no longer in flight; the logical chunk remains pending in _retry.
                _queue->complete(1);
                LOG(DEBUG) << "Retrying object chunk " << cs.chunk.handle << " (offset " << cs.chunk.offset
                           << ", bytes " << cs.chunk.bytesize << ") after " << retry->delay.count()
                           << " ms; application retry " << retry->retry_count;
                progressed = true;
                continue;
            }
            // The retry budget is disabled/exhausted. The internal marker must never escape to callers.
            ret = common::ResponseCode::FileAccessError;
        }

        complete_chunk(wlit, chunk_idx, ret);
        progressed = true;
    }

    // The responder signalled it ran dry (empty round or end-of-round sentinel) while chunks are still in
    // flight: it was stopped or drained early. Abort rather than spin re-reading the same sentinel. Gated
    // on responder_drained, not merely !progressed, so a round that only dropped a late completion (the
    // client is alive) never aborts an unrelated in-flight submission.
    if (!progressed && responder_drained && _queue != nullptr && !_queue->idle())
    {
        abort_all(common::ResponseCode::FinishedError);
    }
}

}; // namespace runai::llm::streamer::impl
