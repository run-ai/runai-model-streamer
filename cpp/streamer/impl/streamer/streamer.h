
#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "utils/threadpool/threadpool.h"
#include "utils/fdlimit/fdlimit.h"

#include "common/responder/responder.h"
#include "common/s3_credentials/s3_credentials.h"
#include "streamer/impl/config/config.h"
#include "streamer/impl/workload/workload.h"
#include "streamer/impl/s3/s3.h"
#include "streamer/impl/batches/batches.h"
#include "streamer/impl/request/request.h"
#include "streamer/impl/submissions/submissions_mgr.h"
#include "streamer/impl/pools/backend_pools.h"
#include "posix_io/mount_capabilities/mount_capabilities.h"
#include "streamer/impl/async_io/async_io_stats/async_io_stats.h"
#include "streamer/impl/async_io/async_io_worker/async_io_worker.h"
#include "streamer/impl/strategy_resolver/strategy_resolver.h"

namespace runai::llm::streamer::impl
{

// Streamer for reading large files concurrently

// The user-facing responder is PERSISTENT and lives for the streamer's lifetime; many
// submissions share it, demuxed by submission_id. response(timeout, done) is the single consumer: it
// blocks / times out (no finish-on-drain) and reports completion per submission via submission_done.

// Synchronous read -  read a range of a file to a given buffer of host memory
// Asynchronous read - read a range of a file to a given buffer of host memory in two stages:
//                          1. request to read a range, specifying a list of sub ranges
//                          2. wait for a response for the next ready sub range
//                     Responses are returned without any promissed order - a response is returned when a sub range is completed

struct Streamer
{
    // How the mount of a path is found. A test can replace it, for the same reason AsyncIoWorker
    // takes an EngineFactory and ObjectStorageWorker takes a credentials provider: the real answer
    // comes from the kernel, and a test cannot create a second mount without CAP_SYS_ADMIN.
    //
    // Production passes nothing. A test passes its own map. It can then run a submission that covers
    // several mounts, and check the path from st_dev to group to engine. Without this seam that path
    // can only be tested on a machine that already has two filesystems.
    //
    // It must throw when a path cannot be read, as MountCapabilities::of_path does.
    using MountProbe = std::function<posix_io::MountCapability(const std::string &)>;

    // Whether a mount can serve O_DIRECT. Replaceable for the same reason as MountProbe, and more
    // sharply: the real answer is whatever the build machine's filesystem happens to support, so a
    // test that used it would pass or fail depending on where it ran.
    //
    // Production passes nothing and MountCapabilities::direct_support answers.
    using DirectProbe =
        std::function<posix_io::DirectSupport(dev_t, const std::string &)>;

    // This mount's measured direct-I/O block, replaceable for the same reason as DirectProbe: the
    // real answer is whatever the build machine's filesystem reports, so a test that used it would
    // assert against a different number depending on where it ran. 0 means "serves no direct reads".
    //
    // Production passes nothing and MountCapabilities::direct_block answers.
    using DirectBlockProbe = std::function<size_t(dev_t, const std::string &)>;

    // What a test may answer instead of the machine.
    //
    // Grouped rather than taken as three more parameters. Each is a std::function, so positionally
    // they are three interchangeable nulls at a call site - `Streamer(Config(), nullptr, nullptr, f)`
    // says nothing about which seam `f` is. A named field says it.
    //
    // Every field is empty in production, and an empty field means "ask the machine". So the default
    // Environment is exactly today's behaviour and nothing has to opt out.
    struct Environment
    {
        // The mount a path is on. A test cannot create a second mount without CAP_SYS_ADMIN, so
        // without this the routing from st_dev to group to engine can only be tested on a machine
        // that already has two filesystems.
        MountProbe mount;

        // Whether a mount serves O_DIRECT. The true answer is whatever the build machine offers, so a
        // test using it would pass or fail depending on where it ran.
        DirectProbe direct;
        DirectBlockProbe direct_block;

        // The engine each async worker builds. Without it a test cannot make an engine FAIL: the real
        // ones only fail when the ring or the context is gone, which no test can arrange, and the
        // behaviour that follows a failure - the mount dropping to the synchronous reader - is the part
        // most worth testing.
        AsyncIoWorker::EngineFactory engine;

        // Whether a strategy can be served here. Needed because every strategy is available on a
        // normal host, so there is no candidate a test can use to reach a failed resolution.
        StrategyResolver::Availability availability;
    };

    Streamer();
    explicit Streamer(Config config, Environment environment = {});
    ~Streamer();

    // Set the streamer's object-storage credentials (a general key->value dictionary; see
    // common::s3::Credentials). Set-once and thread-safe: the first call stores them; a later call with the
    // SAME credentials returns Success; a later call with DIFFERENT credentials returns CredentialsAlreadySet
    // (the streamer's client may already be built from the first set). Credentials are streamer-scoped, not
    // per request - async_request/list_files use whatever was set here.
    common::ResponseCode set_credentials(const common::s3::Credentials & credentials);

    // Submit a read request: a list of files, each with the ranges to read from it. A range is an
    // arbitrary (offset, size) within its file with its own destination - ranges need not be contiguous
    // in the file, contiguous in memory, or ordered.
    // Exactly one response is issued per range, including for a zero-sized range (which is completed
    // immediately without reaching storage). A file with no ranges contributes no responses.
    common::ResponseCode async_request(
      std::vector<FileRanges> & request,
      SubmissionId * out_submission_id = nullptr);

    // Consume the next ready sub-range response over the persistent responder. Blocks up to timeout_ms
    // (0 = indefinitely) and returns TimedOut on expiry; FinishedError only on teardown (stop) - there is no
    // finish-on-drain. On a real response it consumes the owning submission's registry record and sets
    // submission_done to true iff it was that submission's last response (see consume_submission_response).
    // The response carries the submission_id.
    common::Response response(unsigned timeout_ms, bool & submission_done);

    // Which filesystem strategy this streamer resolved to. Valid only after the first submission -
    // resolution happens there, not at construction.
    //
    // Exposed because "which path served this read" is otherwise invisible: the pools are internal
    // and the two paths return identical data, so a test asserting only the bytes cannot tell an
    // io_uring read from a synchronous one.
    // Set the filesystem strategy candidates. Set-once, and rejected once resolution has happened -
    // see StrategyResolver::set_candidates for why one rule is not enough.
    common::ResponseCode set_fs_strategy(const std::string & candidates);

    // Valid only after a FILESYSTEM submission: an object-storage one never resolves a strategy.
    posix_io::Strategy fs_strategy() const;

    // Whether any workload was actually routed to the async pool. fs_strategy() says what was
    // CHOSEN; this says what was USED, and only the second catches a dispatch that ignores the
    // choice.
    bool async_pool_used() const;

    // How many async engines exist. One per mount, up to RUNAI_STREAMER_FS_MAX_ENGINES. Above that
    // limit mounts share an engine, so this can be smaller than the number of mounts read.
    unsigned async_engines() const;

    // What each submission did, and which reader served each of its files. Kept for the last few
    // submissions - see AsyncIoStats.
    const AsyncIoStats & stats() const;

    // What the async workers have done, summed over all of them and over the streamer's whole life.
    //
    // SEPARATE from stats(), and not a field on it, because the scopes differ. A SubmissionStats
    // describes one submission and is recorded when it is dispatched; these are measured during the
    // reads and belong to a worker, which serves many submissions at once.
    //
    // Zero when no async workload has run - there is nothing to sum.
    AsyncIoCounters async_counters() const;

    // For testing only. Credentials are streamer-scoped: call set_credentials first (these use whatever
    // was set there).

    // single synchronous read request from offset in file
    // returns common::ResponseCode::Success if successful or error code
    common::ResponseCode sync_read(const std::string & path, size_t offset, size_t bytesize, void * dst);

    // async request to read a range asynchronously as multiple chunks
    // returns common::ResponseCode::Success if successful or error code
    common::ResponseCode async_read(const std::string & path, size_t offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes);

    // List files under prefix, which may be an object storage URI or a local filesystem path.
    // Applies fnmatch allow/ignore filtering (empty vectors mean no filter) and returns
    // (full path, size) pairs. Uses the streamer's credentials (set_credentials). Throws common::Exception
    // on error.
    std::vector<std::pair<std::string, size_t>> list_files(
      const std::string & prefix,
      bool is_recursive,
      const std::vector<std::string> & allow_patterns,
      const std::vector<std::string> & ignore_patterns);

    // The block a caller must lay destinations out at for THESE paths: the largest any of their
    // mounts requires.
    //
    // Not per path, and it does not need to be. Congruence at a power of two implies congruence at
    // every smaller one, so one number satisfies every mount a request touches even though each mount
    // gets its own engine and may use a smaller block internally.
    //
    // Success        out_block is measured. FileAccessError from a mount that refuses O_DIRECT
    //                contributes nothing, which is right: it imposes no padding requirement.
    // UnknownError   nothing could be measured. out_block is the host page size - a layout value, so
    //                the caller can still place its buffers - and the caller should ask again next
    //                submission rather than treat it as final.
    common::ResponseCode direct_block_for(const std::vector<std::string> & paths, size_t & out_block);

 private:
    // Try to parse path as an object storage URI; returns nullptr for a filesystem path
    std::shared_ptr<common::s3::StorageUri> try_parse_uri(const std::string & path);

    // Reject a submission that mixes backends, and lock the streamer to a single object-storage plugin
    // (first object-storage submission wins). A submission must be either wholly filesystem or wholly one
    // object-storage plugin; the STREAMER may serve both kinds across different submissions (BackendPools
    // keeps one pool per kind). Returns UnsupportedBackendMix when a submission mixes filesystem with
    // object storage, mixes two object-storage plugins, or uses a plugin differing from the lock; else
    // Success.
    common::ResponseCode lock_object_plugin(const std::vector<FileRanges> & request);

    // Which pool group serves each file, indexed by file index: -1 for the synchronous pool, or an
    // async group. There is one async group per mount, because each engine serves one mount and a
    // workload goes to one engine.
    //
    // This runs BEFORE the batches are built. It calls stat once per directory, not once per file. A
    // model with 200 shards keeps them in one directory, so that is one system call instead of 200.
    //
    // out_devices receives the st_dev of each async group, indexed by group id. That value is how the
    // engine is chosen when the workload is sent.
    //
    // This never fails a submission. If a directory cannot be read, that file goes to the synchronous
    // reader, which is where it would have gone anyway.
    //
    // out_blocks is dense with out_devices: the direct-I/O block measured for that mount, or 0 when
    // no file on it could be probed. It travels to the engine so its Limits describe the mount it
    // actually serves rather than a process-wide assumption.
 private:
    std::vector<int> file_groups(const std::vector<FileRanges> & request,
                                 std::vector<dev_t> & out_devices,
                                 std::vector<size_t> & out_blocks);

    // Can this file be read directly, on this mount? Asked only for libaio, and per file.
    //
    // libaio is asynchronous only with O_DIRECT. When a file cannot be read directly the worker opens
    // it buffered, and a buffered read inside io_submit runs inline - one thread, one file at a time,
    // in place of a 16-thread pool with kernel readahead (design 5.7.2). So such a file has to reach
    // the synchronous reader instead, and the decision has to be made HERE: once a workload is on the
    // async pool the worker cannot hand it back.
    //
    // io_uring needs none of this. It falls back to a buffered read on the same ring, which is still
    // asynchronous, so its files stay where they are.
    //
    // Two ways to fail, and they are found in different places. Congruence is a property of the
    // request - the destination and the file offset must leave the same remainder - so it is computed
    // here. O_DIRECT support is a property of the mount and is probed once per mount.
    //
    // Unknown support counts as yes. It means the probe could not open the file at all, which the
    // read is about to report properly; refusing the mount on that basis would send every other file
    // beside it to the synchronous reader too.
    bool reads_directly(const FileRanges & file, dev_t device);

    // Whether this submission reads object storage. The first file WITH RANGES decides, as everywhere
    // else: lock_object_plugin has already rejected a submission that mixes the two, so it is
    // homogeneous by the time anything asks.
    bool is_object_storage_submission(const std::vector<FileRanges> & request);
    // Build the object-storage params for a batch. Credentials are NOT included here (they are read only at
    // client creation, from credentials()); the batch params carry the URI, which is all the per-read path uses.
    common::s3::S3ClientWrapper::Params handle_s3(unsigned file_index, const std::string & path);

    // The streamer's credentials (empty Credentials if none set), read via _credentials_state (which locks
    // its own mutex). Called only at client-creation points (the worker's first client build, and list_files)
    // - never on the per-request path.
    common::s3::Credentials credentials() const;
    void verify_requests(std::vector<FileRanges> & request);

    // Account for one consumed response of submission_id (delegates to _submissions): on the
    // submission's last response, log per-submission throughput. Returns true iff it was the
    // submission's last response (i.e. submission_done).
    bool consume_submission_response(SubmissionId submission_id);

    // Drain workloads[from .. end] as UnknownError so the responder/registry reach zero and the
    // consumer does not hang, when dispatch fails after increment(). `from` is the index of the
    // workload whose push threw: workloads[0 .. from) were already moved into the pool and are
    // NEVER referenced here (excluded by position); workloads[from] is intact because Workload's
    // move is noexcept (see the static_assert in async_request), so push_back's strong guarantee
    // means a throwing dispatch never moved it. Best-effort - under severe OOM a push may throw too.
    void drain_undispatched(SubmissionId submission_id, std::vector<Workload> & workloads, size_t from);

 private:
    std::shared_ptr<const Config> _config;

    // Streamer-scoped object-storage credentials with their own (encapsulated) mutex. Set-once: the first
    // set wins; setting the same value again succeeds; a different value is rejected (CredentialsAlreadySet).
    // Held via shared_ptr so the object-storage workers' credentials provider (which reads them at
    // client-creation time) keeps the state alive regardless of Streamer/worker destruction order. Declared
    // BEFORE _pools so it exists when the pool factory captures it.
    class CredentialsState
    {
     public:
        // Store the credentials (first call), or verify they match a previously-stored set. Returns Success
        // if stored or unchanged; CredentialsAlreadySet if a different value was already set.
        common::ResponseCode set(const common::s3::Credentials & credentials);
        // The stored credentials (empty Credentials if none set yet).
        common::s3::Credentials get() const;

     private:
        mutable std::mutex _mutex;
        std::optional<common::s3::Credentials> _credentials;
    };
    std::shared_ptr<CredentialsState> _credentials_state = std::make_shared<CredentialsState>();

    // Which filesystem strategy this streamer uses. Settled on the first submission, beside the
    // object-storage plugin lock, and never revisited.
    //
    // shared_ptr and declared BEFORE _pools, for the same reason as the credentials above: the async
    // pool's factory reads the resolved strategy when it builds its worker, and holding the state by
    // value keeps it alive regardless of destruction order. Neither factory captures `this`.
    std::shared_ptr<StrategyResolver> _strategy_resolver;

    // Mount capabilities, probed once per mount and cached. Consulted per submission to decide which
    // files the async pool serves - tmpfs goes to the synchronous pool however the strategy resolved.
    posix_io::MountCapabilities _mounts;

    AsyncIoStats _stats;

    // Empty in production, where the machine answers directly.
    //
    // Declared BEFORE _pools, and that order is load-bearing: the async pool's factory reads
    // _environment.engine when it is constructed, and this member is initialised by moving the
    // constructor's argument - so a capture taken from the argument instead would be empty.
    Environment _environment;

    // Every async worker built by the pool factory, so their counters can be summed.
    //
    // shared_ptr and declared BEFORE _pools, for the same reason as the resolver and the credentials:
    // the factory captures it by value and never `this`, so the registry outlives the workers whatever
    // the destruction order.
    std::shared_ptr<AsyncIoWorkers> _async_workers = std::make_shared<AsyncIoWorkers>();

    // Mounts whose asynchronous engine has failed for good.
    //
    // An engine failure is permanent and belongs to ONE mount: the ring or the context is gone, and no
    // later call brings it back. But the files are still readable - the synchronous pool needs none of
    // that machinery - so this is a demotion, not a loss. file_groups() consults it and routes such a
    // mount to the synchronous reader for the rest of the process.
    //
    // Written by a worker thread and read by whichever thread submits, so it owns its own mutex.
    //
    // shared_ptr and declared BEFORE _pools, for the same reason as the registry above: the factory
    // captures it by value, never `this`, so it outlives the workers whatever the destruction order.
    class DeadMounts
    {
     public:
        void add(dev_t device);
        bool contains(dev_t device) const;

     private:
        mutable std::mutex _mutex;
        std::set<dev_t> _devices;
    };
    std::shared_ptr<DeadMounts> _dead_mounts = std::make_shared<DeadMounts>();

    std::unique_ptr<S3Cleanup> _s3;
    // Lazily-created worker pools, one per backend kind. Occupies the slot the single ThreadPool used
    // to, so object-storage workers still join between _s3_stop (S3Stop) and _s3 (S3Cleanup) on teardown.
    BackendPools _pools;
    std::unique_ptr<S3Stop> _s3_stop;
    std::unique_ptr<utils::FdLimitSetter> _fd_limit;
    std::shared_ptr<common::Responder> _responder;

    // Lazy S3 init, each part exactly once in the streamer's lifetime and only for s3 paths.
    // Split because _s3 is shared by list_files and streaming, while fd limit / stop are
    // streaming-only. std::call_once is thread-safe for concurrent submitters and retries if the
    // callable throws (InsufficientFdLimit), so the error resurfaces on the next s3 submission.
    std::once_flag _s3_stream_init_flag;   // fd limit + S3Stop (streaming only)
    std::once_flag _s3_cleanup_init_flag;  // S3Cleanup (list_files and streaming)

    // per-submission bookkeeping (id allocation + completion + throughput); owns its own mutex
    SubmissionsMgr _submissions;
};

}; // namespace runai::llm::streamer::impl
