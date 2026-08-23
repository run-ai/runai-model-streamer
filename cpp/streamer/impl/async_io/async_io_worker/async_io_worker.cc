// O_DIRECT is a GNU extension, so this must come before any libc header.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "streamer/impl/async_io/async_io_worker/async_io_worker.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "common/posix_io/alignment/alignment.h"
#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

namespace
{

// How long a wait may block before returning empty so the worker can notice `stopped`.
//
// 50 ms, a constant rather than a knob. It has no throughput role - with requests outstanding the wait
// returns on the first completion, never at the timeout - so it only bounds how long teardown takes to
// be noticed. An idle wakeup costs a syscall return, so anything from 1 ms to 200 ms is defensible;
// there is nothing here a measurement would resolve.
constexpr unsigned WaitTimeoutMs = 50;

} // namespace

AsyncIoWorker::AsyncIoWorker(common::posix_io::Strategy strategy, EngineFactory factory) :
    _strategy(strategy),
    _factory(std::move(factory))
{
    ASSERT(common::posix_io::is_async(strategy))
        << strategy << " is served by the synchronous pool, not by an engine";
}

AsyncIoWorker::~AsyncIoWorker()
{
    // The pool drains before joining, so this should find nothing. Closing anyway: a descriptor leaked
    // per workload would exhaust the process over a long-lived streamer, and this is the only owner.
    for (auto & [id, wl] : _inflight)
    {
        (void)id;
        for (auto & batch_fd : wl.fds)
        {
            if (batch_fd.fd >= 0)
            {
                ::close(batch_fd.fd);
            }
        }
    }
}

std::size_t AsyncIoWorker::capacity(const Workload & first)
{
    // An empty workload carries no config to size anything from. The streamer skips these before
    // dispatch, so this is not a production path - throw and let the base discard it.
    if (first.batches().empty())
    {
        LOG(WARNING) << "Async io worker received an empty workload";
        throw common::Exception(common::ResponseCode::EmptyRequestError);
    }

    // Resolved HERE, not at construction: depth is divided by RUNAI_STREAMER_PROCESS_GROUP_SIZE, and
    // the window size the base wants IS the engine's depth. One moment, one place.
    _settings.emplace(*first.batches().front().config);

    common::posix_io::AsyncIoConfig config;
    config.depth = _settings->depth();
    config.chunk_bytesize = _settings->chunk_bytesize();

    _engine = _factory(_strategy, config);
    if (_engine == nullptr)
    {
        // Permanent, not transient: a blocked io_uring stays blocked. Reaching this means the
        // dispatcher created an async pool for a host that cannot serve one, which is a bug in
        // strategy resolution rather than a condition to recover from.
        LOG(ERROR) << "No engine for " << _strategy << " - this host cannot serve the strategy it was"
                   << " dispatched for";
        _engine_error = common::ResponseCode::UnknownError;
        throw common::Exception(common::ResponseCode::UnknownError);
    }

    LOG(INFO) << "Async io worker ready: " << _strategy << ", " << *_settings;

    // Harvest into a buffer sized once - nothing is allocated while completing.
    _completions.resize(_engine->depth());

    return _engine->depth();
}

void AsyncIoWorker::discard(Workload && workload)
{
    // The window never came up, so no chunk of this will ever be read. Its responses are already owed,
    // so finalize it here - dropping it hangs the consumer forever.
    //
    // An empty workload has no batches to report to, and it is why capacity() threw in the first
    // place, so it is reported as such rather than as an engine failure.
    const auto code = workload.batches().empty() ? common::ResponseCode::EmptyRequestError : _engine_error;

    Inflight wl;
    wl.workload = std::move(workload);
    report_workload(wl, code);
}

void AsyncIoWorker::enqueue(Workload && workload)
{
    const auto workload_id = _next_workload_id++;

    auto [it, inserted] = _inflight.emplace(workload_id, Inflight{});
    ASSERT(inserted) << "duplicate workload id " << workload_id;

    Inflight & wl = it->second;
    wl.workload = std::move(workload);
    wl.fds.resize(wl.workload.batches().size());   // nothing is opened yet

    size_t queued_chunks = 0;

    for (unsigned batch_index = 0; batch_index < wl.workload.batches().size(); ++batch_index)
    {
        auto & batch = wl.workload.batches()[batch_index];

        // Zero-size tasks appear in no chunk, so nothing will ever complete for them - but a
        // zero-sized range owes a response like any other.
        for (const auto & task : batch.tasks)
        {
            if (task.info.bytesize == 0)
            {
                common::backend_api::Response resp(common::ResponseCode::Success);
                batch.handle_response(resp, &task);
            }
        }

        for (const auto & chunk : batch.chunks)
        {
            const auto id = _chunks.add(chunk, workload_id, batch_index);
            _queue->enqueue(QueuedChunk{ id, chunk.offset, chunk.bytesize, chunk.buffer }, 1);   // cost 1
            ++queued_chunks;
        }
    }

    wl.remaining_chunks = queued_chunks;

    // Nothing to read: every task was zero-sized and has already been answered.
    if (wl.remaining_chunks == 0)
    {
        finalize(it, common::ResponseCode::Success);
    }
}

bool AsyncIoWorker::wants_direct(size_t file_offset, const char * buffer) const
{
    if (!common::posix_io::is_direct(_strategy))
    {
        return false;
    }

    // Congruence, not alignment. A caller can align its buffer and still be out of step with the file,
    // because the file offset is fixed by the file's own layout. When that happens no part of the
    // region can be read directly, so O_DIRECT would copy every byte on this one thread. Buffered I/O
    // copies too, and the kernel does it, and adds readahead - so buffered wins.
    const auto block = common::posix_io::block_size(_engine->limits());

    if (!common::posix_io::is_congruent(file_offset, buffer, block))
    {
        LOG(DEBUG) << "Reading buffered: offset " << file_offset << " and destination are not"
                   << " congruent for block " << block << ", so no part of this file could be read"
                   << " directly";
        return false;
    }

    return true;
}

int AsyncIoWorker::fd_for(Inflight & wl, unsigned batch_index, size_t file_offset, const char * buffer,
                          common::ResponseCode & out_error)
{
    BatchFd & entry = wl.fds[batch_index];

    if (entry.error != common::ResponseCode::Success)
    {
        out_error = entry.error;   // already failed to open; do not try again per chunk
        return -1;
    }

    if (entry.fd >= 0)
    {
        return entry.fd;
    }

    const auto & path = wl.workload.batches()[batch_index].path;

    if (wants_direct(file_offset, buffer))
    {
        entry.fd = ::open(path.c_str(), O_RDONLY | O_DIRECT);
        if (entry.fd >= 0)
        {
            entry.direct = true;
            return entry.fd;
        }

        // The mount refused it. Some filesystems have no O_DIRECT at all, and tmpfs accepts the open
        // and fails later. Falling back for THIS FILE only: another file on another mount is
        // unaffected, which is the whole reason the mode is per file rather than per engine.
        LOG(WARNING) << "Cannot open " << path << " with O_DIRECT (" << std::strerror(errno)
                     << "); reading it buffered";
    }

    entry.fd = ::open(path.c_str(), O_RDONLY);
    if (entry.fd < 0)
    {
        LOG(ERROR) << "Failed to open " << path << " : " << std::strerror(errno);
        entry.error = common::ResponseCode::FileAccessError;   // this file's failure, not the storage's
        out_error = entry.error;
        return -1;
    }

    entry.direct = false;
    return entry.fd;
}

void AsyncIoWorker::stage_pending(common::posix_io::RequestId id)
{
    const auto * entry = _chunks.find(id);
    ASSERT(entry != nullptr) << "staging request " << id << " with no in-flight record";

    const auto wlit = _inflight.find(entry->workload_id);
    if (wlit == _inflight.end())
    {
        // Its workload was aborted while this sat in the queue. Nothing left to answer for it; give
        // back the window credit and forget it.
        _queue->complete(1);
        _chunks.release(id);
        return;
    }

    // pending(), not the chunk's own extent: after a short read this is the remainder, resumed where
    // the last pass stopped. On the first pass they are the same.
    //
    // Read BEFORE the open, because the open needs it: whether a direct read is possible depends on
    // this chunk's offset and destination, and the file is opened lazily on the first chunk exactly so
    // that the answer is available in time.
    const auto pending = _chunks.pending(id);

    auto error = common::ResponseCode::Success;
    const int fd = fd_for(wlit->second, entry->batch_index, pending.offset, pending.buffer, error);
    if (fd < 0)
    {
        complete_chunk(id, error);
        return;
    }

    // What the file WAS opened as, not what the strategy wanted. A direct open can fall back per file.
    const common::posix_io::FileRef file{ fd, wlit->second.fds[entry->batch_index].direct };

    const auto ret = _engine->stage(id, file, pending.offset, pending.bytesize, pending.buffer);
    if (ret != common::ResponseCode::Success)
    {
        // Not staged, so no completion will arrive for it - this worker has to resolve it.
        complete_chunk(id, ret);
    }
}

void AsyncIoWorker::submit(const QueuedChunk & chunk)
{
    stage_pending(chunk.id);
}

void AsyncIoWorker::drain_batch(std::atomic<bool> & stopped)
{
    if (stopped)
    {
        abort_all(common::ResponseCode::FinishedError);
        return;
    }

    // Flush first: the previous pump may have left a backlog, and issuing it before waiting is what
    // stops it sitting a whole loop iteration longer than it must.
    unsigned issued = 0;
    const auto flushed = _engine->flush(issued);
    if (flushed != common::ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to issue staged reads: " << flushed;
        abort_all(flushed);
        return;
    }
    _issued += issued;

    // Block only when something is actually ISSUED. Staged is not issued, so waiting with nothing in
    // flight waits on an empty ring for a completion that will never arrive.
    const auto mode = (_issued > 0) ? common::posix_io::WaitMode::Block
                                    : common::posix_io::WaitMode::NonBlocking;

    unsigned count = 0;
    const auto ret = _engine->wait_for_completions(_completions.data(), _completions.size(), count, mode, WaitTimeoutMs);
    if (ret != common::ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to harvest completions: " << ret;
        abort_all(ret);
        return;
    }

    for (unsigned i = 0; i < count; ++i)
    {
        const auto & completion = _completions[i];

        // Decremented HERE, before any path that might drop this completion. quiesce() waits on
        // _issued reaching zero, so a completion that is dropped without decrementing would leave
        // the count permanently high and hang teardown - the trap
        // design_object_storage_quiesce.md names, where the fix produces the very hang it exists to
        // prevent. Every completion counts, whether or not it can be routed.
        ASSERT(_issued > 0) << "more completions than were issued";
        --_issued;

        const auto * entry = _chunks.find(completion.id);
        if (entry == nullptr)
        {
            // Ids are never reused, so this cannot be a live request - it is a completion for
            // something already resolved or abandoned. Drop it rather than aborting everything else
            // over a stray event.
            LOG(DEBUG) << "Dropping completion for unknown request " << completion.id;
            continue;
        }

        if (completion.ret != common::ResponseCode::Success)
        {
            complete_chunk(completion.id, completion.ret);
            continue;
        }

        switch (_chunks.record(completion.id, completion.bytes_transferred))
        {
        case Progress::Complete:
            complete_chunk(completion.id, common::ResponseCode::Success);
            break;

        case Progress::Eof:
            // Zero further bytes while bytes were still owed: the file is shorter than the caller
            // asked for. Not success, however healthy the read looked.
            complete_chunk(completion.id, common::ResponseCode::EofError);
            break;

        case Progress::Partial:
            // Re-stage the remainder on the same request. The window credit is still held, so this
            // does not go back through the queue - it is the same path submit() takes, resumed.
            stage_pending(completion.id);
            break;
        }
    }
}

void AsyncIoWorker::complete_chunk(common::posix_io::RequestId id, common::ResponseCode ret)
{
    const auto * entry = _chunks.find(id);
    ASSERT(entry != nullptr) << "completing request " << id << " twice";

    const auto workload_id = entry->workload_id;
    const auto batch_index = entry->batch_index;
    const auto chunk = _chunks.release(id);

    // Free the window slot - once per chunk, never on a re-stage.
    //
    // THIS RELEASE POINT MOVES when pinned staging buffers arrive (5.2.4). Reads land directly in the
    // caller's destination today, so the read completing IS the chunk being done. Once a chunk lands
    // in a staging buffer and is copied to the device, the buffer is held until the COPY retires -
    // cuMemcpyHtoDAsync being asynchronous does not release it - and releasing here would hand the
    // buffer to the next chunk while the DMA is still reading out of it. The window is then sized by
    // the pinned pool rather than by the ring depth.
    _queue->complete(1);

    const auto wlit = _inflight.find(workload_id);
    if (wlit == _inflight.end())
    {
        return;   // aborted while this was in flight
    }

    Inflight & wl = wlit->second;
    auto & batch = wl.workload.batches()[batch_index];

    // One read carried every task in the span, so they share its outcome.
    for (unsigned i = 0; i < chunk.task_count; ++i)
    {
        const auto & task = batch.tasks[chunk.first_task + i];

        // Zero-sized tasks were answered at enqueue. They can fall inside a span, and answering one
        // twice is harmless (finished_request is idempotent), but skipping is clearer than relying on
        // that.
        if (task.info.bytesize == 0)
        {
            continue;
        }

        if (ret == common::ResponseCode::Success)
        {
            common::backend_api::Response resp(common::ResponseCode::Success);
            batch.handle_response(resp, &task);
        }
        else
        {
            wl.error_by_file_index.emplace(batch.file_index, ret);   // first error per file
        }
    }

    // Once per chunk, after its tasks are answered. Counting chunks rather than tasks keeps this
    // independent of which tasks the span happened to contain.
    ASSERT(wl.remaining_chunks > 0) << "completing a chunk of a workload with none outstanding";
    if (--wl.remaining_chunks == 0)
    {
        finalize(wlit, common::ResponseCode::Success);   // wl is gone after this
    }
}

void AsyncIoWorker::report_workload(Inflight & wl, common::ResponseCode code)
{
    for (auto & batch : wl.workload.batches())
    {
        auto error = code;
        if (error == common::ResponseCode::Success)
        {
            const auto it = wl.error_by_file_index.find(batch.file_index);
            error = (it == wl.error_by_file_index.end()) ? common::ResponseCode::Success : it->second;
        }
        batch.handle_error(error);
    }
}

void AsyncIoWorker::finalize(InflightMap::iterator it, common::ResponseCode code)
{
    report_workload(it->second, code);

    for (auto & batch_fd : it->second.fds)
    {
        if (batch_fd.fd >= 0)
        {
            ::close(batch_fd.fd);
        }
    }

    _inflight.erase(it);
}

void AsyncIoWorker::quiesce()
{
    // Wait until the kernel holds none of our destinations.
    //
    // A response promises that nothing will write to that range again
    // (design_object_storage_quiesce.md). An ISSUED read has its destination inside the kernel, so
    // reporting its range before the completion arrives hands a live write target to whoever gets
    // the buffer next - under the Python ring, the next submission. Reads that were only staged, or
    // still queued, never reached the kernel and need no wait.
    //
    // UNBOUNDED, terminating only on _issued reaching zero. A timeout here would re-open the exact
    // invariant this exists to protect. A wedged mount therefore hangs its own teardown - which is
    // what one engine per mount (5.2.3) is for: it hangs that engine, not every engine.
    if (_engine == nullptr || _issued == 0)
    {
        return;
    }

    LOG(INFO) << "Waiting for " << _issued << " reads in flight before reporting";

    while (_issued > 0)
    {
        unsigned count = 0;
        const auto ret = _engine->wait_for_completions(_completions.data(), _completions.size(),
                                                       count, common::posix_io::WaitMode::Block,
                                                       WaitTimeoutMs);
        if (ret != common::ResponseCode::Success)
        {
            // The engine cannot tell us any more. Waiting longer cannot make it, and looping here
            // would spin - so stop, and accept that the guarantee is only as good as the engine.
            LOG(ERROR) << "Failed to drain " << _issued << " reads in flight: " << ret
                       << ". Reporting anyway; their destinations may still be written";
            break;
        }

        // Reaped, so the kernel is done with these destinations - which is the whole point. NOT
        // routed: the workload is about to be reported with the abort code, so routing would answer
        // its ranges twice, and a short read must not be re-staged or the drain never ends.
        ASSERT(_issued >= count) << "drained " << count << " completions with " << _issued << " issued";
        _issued -= count;
    }
}

void AsyncIoWorker::abort_all(common::ResponseCode code)
{
    // Drops pending entries and releases their credit in one step - draining through
    // try_take()/complete() would stop at the full-window boundary. First, so nothing new is staged
    // while we wait.
    if (_queue != nullptr)
    {
        _queue->clear();
    }

    // Then wait, and only then report. The order is the whole fix: report-then-abandon declares
    // buffers free while the kernel may still write to them.
    quiesce();

    while (!_inflight.empty())
    {
        finalize(_inflight.begin(), code);
    }

    _chunks.clear();
    _issued = 0;
}

}; // namespace runai::llm::streamer::impl
