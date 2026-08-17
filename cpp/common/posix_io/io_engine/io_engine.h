#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>

#include "common/posix_io/strategy/strategy.h"
#include "common/response_code/response_code.h"

namespace runai::llm::streamer::common::posix_io
{

// Caller-assigned id, and also the in-flight slot index: one worker submits and reaps, so ids can
// index a flat table of `depth` entries instead of needing a map. An index, not a handle - so 32 bits.
using RequestId = uint32_t;

// The file, as the engine sees it. A plain value: the fd belongs to whoever opened it, never to the
// engine.
//
// `direct` travels with each request because the engine keeps no per-file state, and two things need
// it: IOSQE_ASYNC must be set on buffered reads and not on direct ones, and -EINVAL means an
// alignment bug only on a direct fd.
//
// A struct, not a bare fd, so a later field is not a signature change.
struct FileRef
{
    int  fd = -1;
    bool direct = false;   // as opened, after any fallback
};

// What the engine imposes; AsyncIoConfig is what the caller chooses. Read once, at construction.
struct Limits
{
    // Largest single read. Shared by pread, io_prep_pread and IORING_OP_READ. Going over is not an
    // error - the kernel short-reads, and the caller re-stages the rest.
    size_t max_read_bytesize = 0;

    // Advisory, and mirrors stx_dio_offset_align - which governs transfer LENGTH as well as offset.
    // O_DIRECT constrains three things but the kernel reports only two, so there is no third field.
    size_t offset_alignment = 1;   // 1 = no constraint, block size for O_DIRECT
    size_t buffer_alignment = 1;   // 1 = no constraint
};

struct Completion
{
    RequestId    id = 0;
    ResponseCode ret = ResponseCode::Success;

    // The caller MUST compare this against what it asked for. Short reads are routine under
    // io_uring, so treating "no error" as "all bytes arrived" truncates tensors silently.
    size_t bytes_transferred = 0;
};

enum class WaitMode { NonBlocking, Block };

// What the operator chooses. Both fields are clamped to their real ceiling when the engine is built,
// and logged if clamped.
struct AsyncIoConfig
{
    // In-flight requests, PER PROCESS. Configured node-wide and divided by the process group size:
    // with one worker per process, n processes on a node make the device see n x depth.
    unsigned depth = 0;

    // Bytes per request. Any large power of two satisfies offset_alignment. Not the synchronous
    // reader's block size: that 2 MiB floor suits a reader wanting fewer, larger reads.
    size_t chunk_bytesize = 0;
};

// The asynchronous I/O engine: io_uring or libaio, direct or buffered.
//
// Named IoEngine, not Engine - vLLM and SGLang are "engines" too, and this is fio's term
// (--ioengine=io_uring / libaio).
//
// The synchronous reader is NOT one of these: stage/flush/wait model a submission and a completion
// it does not have. It stays as it is, on its own pool.
//
// NOT THREAD SAFE. One worker owns an engine and makes every call on it - which is what keeps this
// free of locks, and what io_uring's ring state needs anyway.
class IoEngine
{
 public:
    virtual ~IoEngine() = default;

    virtual Limits limits() const = 0;
    virtual unsigned depth() const = 0;   // in-flight capacity, after clamping

    // Stage one read of [offset, offset + bytesize) from `file` into `buffer`. May not issue a
    // syscall - see flush().
    //
    // `file.fd` must stay open and `buffer` valid until this id's completion is seen. `id` must be a
    // free slot in [0, depth).
    //
    // `bytesize` is both "bytes wanted" and "bytes you may write": never write past
    // buffer + bytesize. An unaligned length is bounced, not rounded up, or the kernel writes a whole
    // block and overruns a destination sized exactly to the range.
    //
    // A non-Success return means nothing was staged and no completion will arrive; the caller has to
    // resolve the request itself.
    virtual ResponseCode stage(RequestId id, FileRef file, size_t offset, size_t bytesize, char * buffer) = 0;

    // Issue what is staged, in as few syscalls as possible, and report how many went out.
    //
    // Whatever could not be issued stays staged and is retried, in order, on the next call. Never
    // blocks or spins waiting for capacity: only reaping completions frees capacity, and reaping runs
    // on this same thread, so spinning here deadlocks.
    //
    // So out_issued matters, and zero progress is backpressure, not an error.
    virtual ResponseCode flush(unsigned & out_issued) = 0;

    // Collect completions into a caller-owned array - nothing is allocated here.
    //
    // Waits for at least one only when mode == Block, and then at most timeout_ms (0 = forever,
    // matching SharedQueue::pop).
    //
    // An expired timeout is Success with out_count == 0, not an error - the same shape as finding
    // nothing ready. It is also the teardown wake-up: no other thread may touch the engine, so this
    // returning is the only way a waiting worker learns it should stop.
    virtual ResponseCode wait_for_completions(Completion * out, unsigned max, unsigned & out_count,
                                              WaitMode mode, unsigned timeout_ms = 0) = 0;

    // Best-effort cancel of everything in flight. Completions still arrive and must still be drained.
    //
    // io_uring really cancels (IORING_OP_ASYNC_CANCEL, completions come back as -ECANCELED). libaio
    // mostly cannot - io_cancel returns EINVAL once the request is with the driver - so there this
    // means "stop staging and wait". Either way the caller drains; only the latency differs.
    virtual void cancel_all() = 0;

    // Optional: pin a long-lived region once instead of per I/O (io_uring registered buffers).
    // libaio has no equivalent, hence the no-op default.
    //
    // For engine- or streamer-owned memory ONLY - never a caller's destination, and never the Python
    // ring buffer pool. Python allocates and frees that pool and C++ cannot veto the free, while the
    // kernel keeps pinned pages alive after the mapping goes; freeing a still-registered region
    // corrupts the next allocation. Draining I/O does not help - it is a different hazard.
    virtual void register_memory(char * /* base */, size_t /* size */) {}
    virtual void unregister_memory(char * /* base */) {}
};

// Largest read the kernel will do in one go: INT_MAX rounded down to a page.
//
// Computed, never hard-coded as 0x7FFFF000 - that is the 4 KiB value, and a 64 KiB-page kernel
// (RHEL/SUSE aarch64) caps 60 KiB lower, so the constant would be a clamp that does not clamp.
//
// Takes the page size so a test can check the 64 KiB case. Every machine we test on has 4 KiB pages,
// where the formula and the constant look identical - measured: `return 0x7FFFF000;` passed the whole
// suite before this was split in two.
size_t max_read_bytesize(size_t page_size);

// This host's ceiling: max_read_bytesize(sysconf(_SC_PAGESIZE)).
size_t max_read_bytesize();

// Build the engine for this strategy, or return nullptr if the host cannot provide it - a blocked
// io_uring_setup, a missing opcode, an aio context that cannot be sized.
//
// Takes one resolved strategy, not the candidate list: walking the list and recording why each was
// rejected is the dispatcher's job. That way nullptr means one thing only - "not available here" -
// rather than also meaning "no engine was wanted". is_async(strategy) must be true.
//
// Called on the FIRST WORKLOAD, not at streamer construction: depth depends on
// RUNAI_STREAMER_PROCESS_GROUP_SIZE, which Python does not set until stream_files(), long after
// runai_start() returned. Building earlier would read the default of 1 and skip the division.
std::unique_ptr<IoEngine> make_io_engine(Strategy strategy, const AsyncIoConfig & config);

}; // namespace runai::llm::streamer::common::posix_io
