#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "common/posix_io/engine_factory/engine_factory.h"
#include "common/posix_io/io_engine/io_engine.h"
#include "common/posix_io/strategy/strategy.h"
#include "common/response_code/response_code.h"

#include "streamer/impl/async_io/async_io_settings/async_io_settings.h"
#include "streamer/impl/async_io/inflight_chunks/inflight_chunks.h"
#include "streamer/impl/batch/batch.h"
#include "streamer/impl/workload/workload.h"
#include "utils/capacity_worker/capacity_worker.h"

namespace runai::llm::streamer::impl
{

// One chunk waiting in the window, by value.
//
// Deliberately NOT a pointer into Batch::chunks: entries sit in the queue while their workload can be
// erased underneath them (abort_all on stop, a drained responder, an OOM mid-registration), so a
// pointer would be a use-after-free on exactly the paths hardest to exercise. A stale value is merely
// useless and trips an assert. The copy is three fields per in-flight request - about 20 KB at depth
// 512.
//
// Same shape as ObjectStorageWorker's ObjectChunk, so the two can share a base later.
struct QueuedChunk
{
    common::posix_io::RequestId id = 0;
    size_t offset = 0;
    size_t bytesize = 0;
    char * buffer = nullptr;
};

// The single worker driving one IoEngine.
//
// A CapacityWorker, so the base owns the in-flight window and the submit/drain interleave and this
// only fills the backend hooks - the same arrangement ObjectStorageWorker uses, deliberately, so the
// completion routing can be lifted into a shared base once both exist.
//
// ONE worker owns ONE engine and makes every call on it (5.2), which is what keeps this free of locks.
// Under one engine per mount (5.2.3) there are several of these, one per mount, each in its own
// single-worker pool - so nothing here may assume it is the only instance.
class AsyncIoWorker : public utils::CapacityWorker<Workload, QueuedChunk>
{
 public:
    // Builds the engine for a strategy and its resolved configuration, or returns nullptr if this host
    // cannot provide it.
    //
    // Injectable so tests can drive the worker with MockIoEngine - the same shape as
    // ObjectStorageWorker's credentials provider, and for the same reason. Production passes nothing.
    using EngineFactory = std::function<std::unique_ptr<common::posix_io::IoEngine>(
                              common::posix_io::Strategy, const common::posix_io::AsyncIoConfig &)>;

    explicit AsyncIoWorker(common::posix_io::Strategy strategy,
                           EngineFactory factory = common::posix_io::make_io_engine);
    ~AsyncIoWorker() override;

 protected:
    // Builds the engine and returns the window size, on the FIRST workload.
    //
    // Not at construction: depth is divided by RUNAI_STREAMER_PROCESS_GROUP_SIZE, which the Python
    // layer does not publish until stream_files() - long after runai_start() returned. Building
    // earlier reads 1, skips the division, and the device sees the full depth per process.
    //
    // The returned capacity IS the in-flight bound, and the only one: ids are monotonic rather than
    // indices into a fixed table (5.3), so nothing else can disagree about how many may be outstanding.
    //
    // THROWS if no engine can be built, so the base discards this workload rather than dropping it.
    // Unlike object storage - whose client can fail transiently on credentials or a missing plugin -
    // failure here is PERMANENT: a blocked io_uring stays blocked. So the retry the base offers buys
    // nothing, and reaching this at all means the dispatcher created an async pool for a host that
    // cannot serve one. Logged as an invariant violation, not as a condition to recover from.
    std::size_t capacity(const Workload & first) override;

    // The window could not come up. Finalize the workload anyway - its responses are already owed, and
    // dropping it hangs the consumer forever.
    void discard(Workload && workload) override;

    // Register the workload and queue its chunks. Zero-sized tasks are completed here: they appear in
    // no chunk (chunk_splitter.h), so nothing will ever complete for them, and they still owe a
    // response each.
    void enqueue(Workload && workload) override;

    // Stage one chunk. Staging is not issuing - flush() does that - so this must not block.
    void submit(const QueuedChunk & chunk) override;

    // Flush the backlog, harvest with a bounded wait, and route what arrived. On `stopped`, walk BOTH
    // the in-flight set and the staged-but-unissued set: the latter hold window credit and will never
    // produce a completion, so omitting them leaves tasks unfinalized.
    void drain_batch(std::atomic<bool> & stopped) override;

 private:
    // Per workload, kept until its last task is accounted.
    // A batch's descriptor, opened by the first chunk of it that actually issues.
    //
    // Lazy in, bulk out: nothing is opened at enqueue - a stopped submission does none at all, and a
    // zero-size batch has no chunks so is never opened - and everything is closed in one place when
    // the workload ends. A workload is the caller's statement that these files are read in parallel,
    // so holding a descriptor for as long as its workload lives is the honest lifetime.
    //
    // `error` remembers a failed open so the next chunk of the same batch fails immediately instead of
    // retrying the open, and logging, once per chunk.
    struct BatchFd
    {
        int fd = -1;
        common::ResponseCode error = common::ResponseCode::Success;
    };

    struct Inflight
    {
        Workload workload;
        std::vector<BatchFd> fds;   // one per batch, indexed by InflightChunk::batch_index
        // Chunks, not tasks. Every chunk completes exactly once, and every non-empty task is in
        // exactly one chunk - so all chunks done already means all tasks answered, and the zero-sized
        // ones (answered at enqueue, and able to sit inside a span) never enter the arithmetic.
        //
        // ObjectStorageWorker counts tasks because there a task spans several chunks. After the task
        // cut ours is the inverse, so the counter inverts with it.
        size_t remaining_chunks = 0;
        std::map<unsigned, common::ResponseCode> error_by_file_index;   // first error per file
    };

    using InflightMap = std::map<uint64_t, Inflight>;

    // Stage the outstanding part of a request - the whole chunk the first time, the remainder after a
    // short read. Resolves it here if it cannot be staged, since no completion would then arrive.
    void stage_pending(common::posix_io::RequestId id);

    // Push each batch's aggregate result: the whole-workload code if non-Success, else that file's
    // recorded error.
    void report_workload(Inflight & wl, common::ResponseCode code);

    // Account every task a landed chunk covered - one read carried them all, so they share its
    // outcome - and finalize the workload once its last task lands.
    void complete_chunk(common::posix_io::RequestId id, common::ResponseCode ret);

    // The batch's descriptor, opening it on first use. Returns -1 and sets out_error if it cannot be
    // opened - that is this file's failure, not the storage's.
    int fd_for(Inflight & wl, unsigned batch_index, common::ResponseCode & out_error);

    void finalize(InflightMap::iterator it, common::ResponseCode code);

    // Wait until no read is still in flight, so no destination can be written after its range is
    // reported. Unbounded on purpose - see the definition.
    void quiesce();

    void abort_all(common::ResponseCode code);

    const common::posix_io::Strategy _strategy;
    const EngineFactory _factory;

    // Why the engine could not be built, for discard() to report. Reset after use so the next workload
    // retries - though a failure here is permanent in practice.
    common::ResponseCode _engine_error = common::ResponseCode::UnknownError;

    std::optional<AsyncIoSettings> _settings;          // resolved with the engine, on the first workload
    std::unique_ptr<common::posix_io::IoEngine> _engine;

    InflightChunks _chunks;   // id -> chunk, its progress, and where to route it
    InflightMap _inflight;

    uint64_t _next_workload_id = 1;

    // Issued and not yet completed. Tracked SEPARATELY from staged: staged is not issued, so waiting
    // with nothing issued waits on an empty ring for a completion that will never arrive. When this is
    // zero the wait must be NonBlocking.
    size_t _issued = 0;

    // Harvested into on every drain, sized once - nothing is allocated while completing.
    std::vector<common::posix_io::Completion> _completions;
};

}; // namespace runai::llm::streamer::impl
