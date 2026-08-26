// O_DIRECT is a GNU extension, so this must come before any libc header.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "streamer/impl/async_io/async_io_worker/async_io_worker.h"

#include "common/posix_io/completion_mapper/completion_mapper.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
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

    // One block-sized buffer per in-flight read, so a bounced pass can always get one and there is no
    // limit to enforce. At depth 512 and a 4096-byte block that is 2 MB. Only a direct strategy ever
    // bounces, so a buffered engine builds none.
    if (common::posix_io::is_direct(_strategy))
    {
        _scratch = std::make_unique<common::posix_io::ScratchPool>(
            _engine->depth(), common::posix_io::block_size(_engine->limits()));
    }

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
        //
        // The answer depends on whether the FILE can be opened, so the file is checked first. The
        // synchronous reader opens the file for every batch before it looks at any range size, so a
        // zero-sized range in a missing file fails with the file. Both readers must agree: which one
        // served a request is meant to be invisible to the caller.
        //
        // Answering Success without looking was wrong in two ways. A batch whose ranges are ALL
        // zero-sized never opened the file at all, so a missing file reported success. A mixed batch
        // answered its zero-sized ranges Success and its other ranges FileAccessError, for one file.
        const bool has_zero_sized = std::any_of(
            batch.tasks.begin(), batch.tasks.end(),
            [](const auto & task) { return task.info.bytesize == 0; });

        if (has_zero_sized)
        {
            // Only when there IS a zero-sized range, so the usual path keeps its lazy open.
            //
            // Opened and closed rather than kept: fd_for() opens this file later and decides then
            // whether it can use O_DIRECT, which needs a chunk's offset and destination. Keeping this
            // buffered fd would take that decision away from every batch that has a zero-sized range.
            const auto code = probe_open(batch.path);

            if (code == common::ResponseCode::Success)
            {
                for (const auto & task : batch.tasks)
                {
                    if (task.info.bytesize == 0)
                    {
                        common::backend_api::Response resp(common::ResponseCode::Success);
                        batch.handle_response(resp, &task);
                    }
                }
            }
            else
            {
                // Recorded against the FILE, not answered here - the same way every other failure on
                // this path is handled (see complete_chunk). report_workload applies it to every task
                // of this file that is still unanswered when the workload finalizes.
                //
                // Not through handle_response: that THROWS on a non-Success code, because it is the
                // object-storage abort path. Answering an error through it would throw out of
                // enqueue, the workload would never finalize, and the caller would wait for a
                // response that can no longer come.
                wl.error_by_file_index.emplace(batch.file_index, code);
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

bool AsyncIoWorker::plan_direct_pass(const Chunk & pending, size_t block, DirectPass & out)
{
    const size_t head = pending.offset % block;

    if (head != 0)
    {
        // The cursor is inside a block. Read that whole block into scratch and copy out the part that
        // was asked for - which may be the whole rest of the block, or less if the chunk ends inside
        // it.
        char * scratch = _scratch == nullptr ? nullptr : _scratch->take();
        if (scratch == nullptr)
        {
            return false;
        }

        out.offset = pending.offset - head;
        out.bytesize = block;
        out.buffer = scratch;
        out.scratch = scratch;
        out.skip = head;
        out.wanted = std::min(block - head, pending.bytesize);
        return true;
    }

    if (pending.bytesize >= block)
    {
        // On a boundary with at least a block to read: whole blocks, straight into the destination.
        // This is the case that costs nothing, and it is every chunk in the middle of a region.
        out.offset = pending.offset;
        out.bytesize = pending.bytesize - (pending.bytesize % block);
        out.buffer = pending.buffer;
        return true;
    }

    // On a boundary with less than a block left: the tail. One block into scratch, keep what is owed.
    char * scratch = _scratch == nullptr ? nullptr : _scratch->take();
    if (scratch == nullptr)
    {
        return false;
    }

    out.offset = pending.offset;
    out.bytesize = block;
    out.buffer = scratch;
    out.scratch = scratch;
    out.skip = 0;
    out.wanted = pending.bytesize;
    return true;
}

size_t AsyncIoWorker::bounced_bytes() const
{
    return _bounced_bytes;
}

size_t AsyncIoWorker::bytes_read() const
{
    return _bytes_read;
}

common::posix_io::FileRef AsyncIoWorker::file_of(const InflightChunk & entry) const
{
    // The file this request was staged against, rebuilt from what the worker already recorded. The
    // engine cannot supply it: a completion carries only the id.
    //
    // An unknown workload gives an empty FileRef. That happens when the workload was finalized while
    // this read was still out, and the result is only a slightly worse log line - the mapping falls
    // back to what a buffered fd would give.
    const auto wlit = _inflight.find(entry.workload_id);
    if (wlit == _inflight.end() || entry.batch_index >= wlit->second.fds.size())
    {
        return common::posix_io::FileRef{};
    }

    const auto & batch_fd = wlit->second.fds[entry.batch_index];
    return common::posix_io::FileRef{ batch_fd.fd, batch_fd.direct };
}

size_t AsyncIoWorker::land_bounced_pass(common::posix_io::RequestId id, size_t bytes_transferred)
{
    auto * entry = _chunks.find_mutable(id);
    ASSERT(entry != nullptr) << "landing a bounced pass for an unknown request " << id;

    if (entry->scratch == nullptr)
    {
        return bytes_transferred;   // not bounced; the bytes are already where they belong
    }

    // What the kernel gave us minus the part we did not ask for. A short read can land inside the
    // skipped head, in which case nothing wanted arrived at all.
    const size_t useful = bytes_transferred <= entry->scratch_skip
                        ? 0
                        : std::min(entry->scratch_wanted, bytes_transferred - entry->scratch_skip);

    if (useful > 0)
    {
        // pending() gives the destination for the cursor, which is where these bytes belong.
        const auto pending = _chunks.pending(id);
        std::memcpy(pending.buffer, entry->scratch + entry->scratch_skip, useful);

        // Counted where the copy happens, so the number is the bytes actually moved rather than an
        // estimate from the request sizes. A pass reads a whole block but copies only what was asked
        // for.
        _bounced_bytes += useful;
    }

    _scratch->give(_chunks.clear_bounce(id));
    return useful;
}

common::ResponseCode AsyncIoWorker::probe_open(const std::string & path)
{
    // Buffered, always. The question here is only whether the file can be opened, and O_DIRECT would
    // add a second reason to fail that has nothing to do with the file existing - some mounts refuse
    // it outright.
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
    {
        LOG(ERROR) << "Failed to open " << path << " : " << std::strerror(errno);
        return common::ResponseCode::FileAccessError;   // this file's failure, not the storage's
    }

    // The same check fd_for makes, so a path answers the same way whether or not its ranges have
    // bytes in them. Without this a directory would be Success for a zero-sized range and
    // FileAccessError for every other range of the same path.
    const bool readable = readable_file(fd, path);

    ::close(fd);
    return readable ? common::ResponseCode::Success : common::ResponseCode::FileAccessError;
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

    if (!readable_file(entry.fd, path))
    {
        ::close(entry.fd);
        entry.fd = -1;
        entry.error = common::ResponseCode::FileAccessError;
        out_error = entry.error;
        return -1;
    }

    return entry.fd;
}

bool AsyncIoWorker::readable_file(int fd, const std::string & path)
{
    struct stat st;

    if (::fstat(fd, &st) != 0)
    {
        LOG(ERROR) << "Failed to stat " << path << " : " << std::strerror(errno);
        return false;
    }

    if (!S_ISDIR(st.st_mode))
    {
        return true;
    }

    // open(O_RDONLY) succeeds on a directory, so without this check the read is staged and each reader
    // finds out somewhere different. Measured: libaio refuses the whole io_submit with EINVAL,
    // io_uring accepts the read and completes it with -EINVAL, and the synchronous reader gets EISDIR
    // from pread.
    //
    // Answering here makes all three say the same thing, and say it in words.
    //
    // It also removes a dependency on an accident. A directory cannot be opened with O_DIRECT, so its
    // fd is always buffered, and map_completion turns EINVAL on a buffered fd into FileAccessError -
    // one file fails and the rest carry on. On a DIRECT fd the same EINVAL means an alignment bug and
    // maps to UnknownError, which aborts the whole submission. We would be relying on the kernel to
    // keep refusing O_DIRECT on directories.
    LOG(ERROR) << path << " is a directory, not a file";
    return false;
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
    const bool direct = wlit->second.fds[entry->batch_index].direct;
    const common::posix_io::FileRef file{ fd, direct };

    // A buffered read is issued exactly as asked. A direct one is cut to what the kernel will accept,
    // which may mean reading one block into scratch and copying the wanted part out.
    auto pass = DirectPass{ pending.offset, pending.bytesize, pending.buffer, nullptr, 0, 0 };

    if (direct && !plan_direct_pass(pending, common::posix_io::block_size(_engine->limits()), pass))
    {
        // No scratch was free. Not expected - there is one per in-flight read - but failing the whole
        // read over it would be worse than reading a little less this pass.
        LOG(WARNING) << "No scratch buffer for a direct read at offset " << pending.offset
                     << "; the pool is sized to the window, so this should not happen";
        complete_chunk(id, common::ResponseCode::UnknownError);
        return;
    }

    if (pass.scratch != nullptr)
    {
        _chunks.set_bounce(id, pass.scratch, pass.skip, pass.wanted);
    }

    const auto ret = _engine->stage(id, file, pass.offset, pass.bytesize, pass.buffer);
    if (ret != common::ResponseCode::Success)
    {
        // Not staged, so no completion will arrive for it - this worker has to resolve it. Give the
        // scratch back first: nothing else will, and a leaked buffer drains the pool.
        if (char * scratch = _chunks.clear_bounce(id))
        {
            _scratch->give(scratch);
        }

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

        if (completion.failed())
        {
            // Mapped HERE, not in the engine, because mapping needs the file. EINVAL means our
            // alignment rule broke on a direct fd, and means something else on a buffered one
            // (completion_mapper.h). The engine holds only the id when a completion arrives, so it
            // cannot tell the two apart. This worker can: it opened the file and recorded how.
            const auto file = file_of(*entry);
            const auto code = common::posix_io::map_completion(completion.res, file);

            // Give the scratch back before failing, or the pool drains one buffer per failed read.
            if (char * scratch = _chunks.clear_bounce(completion.id))
            {
                _scratch->give(scratch);
            }

            complete_chunk(completion.id, code);
            continue;
        }

        // A bounced pass read a whole block but only part of it was asked for. Converting here, before
        // record(), is what stops the cursor advancing past bytes that never arrived.
        const size_t useful = land_bounced_pass(completion.id, completion.bytes_transferred());
        _bytes_read += useful;

        switch (_chunks.record(completion.id, useful))
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

    // Cumulative for this worker, not for this workload: workloads overlap in one window, so bytes
    // cannot be attributed to one of them. Per-submission numbers need the counters to reach the
    // streamer, which is a later step.
    LOG(DEBUG) << "Async io worker: " << _bytes_read << " bytes read, " << _bounced_bytes
               << " copied through scratch";

    // The number that decides whether direct reads are worth having.
    //
    // Expected near zero: a region copies at most one partial block at each end, so about 0.1% of an
    // 8 MiB region. A large share means destinations are no longer congruent with their file offsets,
    // and then EVERY byte is copied - by this one thread - which is slower than reading buffered.
    //
    // There is no error for this. The reads all succeed. Without this line the only symptom is that
    // the model loads slowly, which is not a symptom anyone can act on.
    // Not judged before enough has been read. A region copies at most one partial block at each end,
    // so a SMALL region is legitimately a large share - a 9 KB region with a 4096 block can be half
    // copied while everything is working exactly as designed. Judging that would warn about nothing on
    // any model with many small tensors.
    //
    // 64 MiB is a few regions at the default chunk size, which is enough for the ratio to mean
    // something.
    constexpr size_t EnoughToJudge = 64ul << 20;

    if (!_warned_about_bouncing && _bytes_read >= EnoughToJudge && _bounced_bytes * 100 > _bytes_read)
    {
        _warned_about_bouncing = true;
        LOG(WARNING) << "More than 1% of bytes read are being copied through a scratch buffer ("
                     << _bounced_bytes << " of " << _bytes_read << "). Destinations are probably not"
                     << " congruent with their file offsets, which makes direct reads copy everything."
                     << " Buffered reads would be faster.";
    }

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

    // Every buffer still held by an abandoned pass. clear() forgets the chunks, so this must come
    // first or the pool would be empty for the rest of the streamer's life.
    if (_scratch != nullptr)
    {
        _chunks.release_all_scratch([this](char * scratch) { _scratch->give(scratch); });
    }

    _chunks.clear();
    _issued = 0;
}

}; // namespace runai::llm::streamer::impl
