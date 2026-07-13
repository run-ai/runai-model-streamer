
#pragma once

#include <memory>
#include <mutex>
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
#include "streamer/impl/submissions/submissions_mgr.h"

namespace runai::llm::streamer::impl
{

// Streamer for reading large files concurrently

// The user-facing responder is PERSISTENT and lives for the streamer's lifetime; many
// submissions share it, demuxed by submission_id. The legacy response() keeps the historical
// finish-on-drain behavior by checking _responder->finished() at the streamer level.

// Synchronous read -  read a range of a file to a given buffer of host memory
// Asynchronous read - read a range of a file to a given buffer of host memory in two stages:
//                          1. request to read a range, specifying a list of sub ranges
//                          2. wait for a response for the next ready sub range
//                     Responses are returned without any promissed order - a response is returned when a sub range is completed

struct Streamer
{
    Streamer();
    Streamer(Config config);
    ~Streamer();

    common::ResponseCode async_request(
      std::vector<std::string> & paths,
      std::vector<size_t> & file_offsets,
      std::vector<size_t> & bytesizes,
      std::vector<void *> & dsts,
      std::vector<unsigned> & num_sizes,
      std::vector<std::vector<size_t>> & internal_sizes,
      const common::s3::Credentials & credentials,
      unsigned * out_submission_id = nullptr);

    // True while any submission still has unconsumed responses (the responder is not drained).
    // Used by the legacy single-request C API (runai_request) to reject an overlapping request
    // with BusyError - a safety net against a caller reusing buffers before draining. The
    // multi-request _ex path does not consult this.
    bool busy() const;

    // return when there is a ready chunk
    // returns common::ResponseCode::FinishedError if no responses are expected
    // returns common::ResponseCode error if failed
    common::Response response();

    // Multi-request consumer over the persistent responder. Unlike response() it does NOT
    // finish on drain: it blocks up to timeout_ms (0 = indefinitely) and returns TimedOut on
    // expiry; FinishedError only on teardown (stop). On a real response it consumes the owning
    // submission's registry record and sets submission_done to true iff it was that submission's
    // last response (see consume_submission_response). The response carries the submission_id.
    common::Response response_ex(unsigned timeout_ms, bool & submission_done);

    // For testing only:

    // single synchronous read request from offset in file
    // returns common::ResponseCode::Success if successful or error code
    common::ResponseCode sync_read(const std::string & path, size_t offset, size_t bytesize, void * dst, const common::s3::Credentials & credentials);

    // async request to read a range asynchronously as multiple chunks
    // returns common::ResponseCode::Success if successful or error code
    common::ResponseCode async_read(const std::string & path, size_t offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes, const common::s3::Credentials & credentials);

    // List files under prefix, which may be an object storage URI or a local filesystem path.
    // Applies fnmatch allow/ignore filtering (empty vectors mean no filter) and returns
    // (full path, size) pairs. Throws common::Exception on error.
    std::vector<std::pair<std::string, size_t>> list_files(
      const std::string & prefix,
      bool is_recursive,
      const std::vector<std::string> & allow_patterns,
      const std::vector<std::string> & ignore_patterns,
      const common::s3::Credentials & credentials);

 private:
    // Try to parse path as an object storage URI; returns nullptr for a filesystem path
    std::shared_ptr<common::s3::StorageUri> try_parse_uri(const std::string & path);
    common::s3::S3ClientWrapper::Params handle_s3(unsigned file_index, const std::string & path, const common::s3::Credentials & credentials);
    void verify_requests(std::vector<std::string> & paths, std::vector<size_t> & file_offsets, std::vector<size_t> & bytesizes, std::vector<unsigned> & num_sizes, std::vector<void *> & dsts);

    // Account for one consumed response of submission_id (delegates to _submissions): on the
    // submission's last response, log per-submission throughput. Returns true iff it was the
    // submission's last response (i.e. submission_done).
    bool consume_submission_response(unsigned submission_id);

    // Drain workloads[from .. end] as UnknownError so the responder/registry reach zero and the
    // consumer does not hang, when dispatch fails after increment(). `from` is the index of the
    // workload whose push threw: workloads[0 .. from) were already moved into the pool and are
    // NEVER referenced here (excluded by position); workloads[from] is intact because Workload's
    // move is noexcept (see the static_assert in async_request), so push_back's strong guarantee
    // means a throwing dispatch never moved it. Best-effort - under severe OOM a push may throw too.
    void drain_undispatched(unsigned submission_id, std::vector<Workload> & workloads, size_t from);

 private:
    std::shared_ptr<const Config> _config;
    std::unique_ptr<S3Cleanup> _s3;
    utils::ThreadPool<Workload> _pool;
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
