
#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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
#include "common/posix_io/mount_capabilities/mount_capabilities.h"
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
    using MountProbe = std::function<common::posix_io::MountCapability(const std::string &)>;

    Streamer();
    explicit Streamer(Config config, MountProbe mount_probe = nullptr);
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
    common::posix_io::Strategy fs_strategy() const;

    // Whether any workload was actually routed to the async pool. fs_strategy() says what was
    // CHOSEN; this says what was USED, and only the second catches a dispatch that ignores the
    // choice.
    bool async_pool_used() const;

    // How many async engines exist. One per mount, up to RUNAI_STREAMER_FS_MAX_ENGINES. Above that
    // limit mounts share an engine, so this can be smaller than the number of mounts read.
    unsigned async_engines() const;

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
    std::vector<int> file_groups(const std::vector<FileRanges> & request,
                                 std::vector<dev_t> & out_devices);

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
    common::posix_io::MountCapabilities _mounts;

    // Null in production, where _mounts answers directly.
    MountProbe _mount_probe;

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
