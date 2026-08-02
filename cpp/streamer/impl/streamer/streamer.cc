#include "streamer/impl/streamer/streamer.h"

#include <fnmatch.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "utils/logging/logging.h"
#include "utils/scope_guard/scope_guard.h"

#include "streamer/impl/workload/workload.h"
#include "streamer/impl/object_storage_worker/object_storage_worker.h"
#include "streamer/impl/assigner/assigner.h"
#include "streamer/impl/batches/batches.h"
#include "common/exception/exception.h"
#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::impl
{

Streamer::Streamer() : Streamer(Config())
{}

Streamer::Streamer(Config config) :
    _config(std::make_shared<Config>(config)),
    // Filesystem reads are synchronous (concurrency threads, stateless handler); object-storage reads are
    // asynchronous (s3_concurrency ObjectStorageWorkers, each owning a client + in-flight capacity window).
    // Pools are created lazily on first use of each kind.
    _pools(
        [](Workload&& workload, std::atomic<bool> & stopped)
        {
            workload.execute(stopped);
        },
        // each object-storage worker reads the streamer's credentials once, at client creation, via this
        // provider. It captures the shared credentials state by value, so the state outlives the worker
        // regardless of destruction order (it never captures `this`).
        [state = _credentials_state]() -> std::unique_ptr<utils::Worker<Workload>>
        {
            return std::make_unique<ObjectStorageWorker>([state]() { return state->get(); });
        },
        _config->concurrency, _config->s3_concurrency),
    // One PERSISTENT responder for the streamer's lifetime, shared by all submissions and
    // demuxed by submission_id. increment() grows its expected count per accepted submission.
    _responder(std::make_shared<common::Responder>(0, common::QueueMode::PERSISTENT))
{
    LOG(DEBUG) << config;
}

Streamer::~Streamer()
{
    try
    {
        LOG(DEBUG) << "Streamer shutting down";
        // unblock any consumer parked in response()/pop() on the persistent responder
        _responder->stop();
    }
    catch(...)
    {}
}

common::ResponseCode Streamer::sync_read(const std::string & path, size_t file_offset, size_t bytesize, void * dst)
{
    LOG(SPAM) << "Requested to read " << bytesize << " bytes from " << path << " offset " << file_offset;

    auto r = async_read(path, file_offset, bytesize, dst, 1, &bytesize);
    if (r != common::ResponseCode::Success)
    {
        return r;
    }

    // consume the single response (blocking); consuming it also forgets the submission's registry record
    bool submission_done = false;
    return response(0, submission_done).ret;
}

common::ResponseCode Streamer::async_read(const std::string & path, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes)
{
    common::ResponseCode ret = common::ResponseCode::Success;

    try
    {
        // This convenience wrapper keeps the classic contiguous semantics: the sub ranges tile
        // [file_offset, file_offset + bytesize) in order, and are written consecutively from dst.
        // The general API can express any layout; here the offsets and destinations are accumulated.
        std::vector<FileRanges> request(1);
        request[0].path = path;
        request[0].ranges.reserve(num_sizes);

        size_t offset = file_offset;
        char * destination = static_cast<char *>(dst);
        for (unsigned i = 0; i < num_sizes; ++i)
        {
            request[0].ranges.push_back(ReadRange{ offset, internal_sizes[i], destination });
            offset += internal_sizes[i];
            destination += internal_sizes[i];
        }

        ret = async_request(request);
    }
    catch(const common::Exception & e)
    {
        LOG(ERROR) << "caught exception " << e.what();
        ret = e.error();
    }

    return ret;
}

common::ResponseCode Streamer::CredentialsState::set(const common::s3::Credentials & credentials)
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);

    if (!_credentials.has_value())
    {
        _credentials = credentials;   // first set wins
        return common::ResponseCode::Success;
    }

    // set-once: re-setting the same credentials is a no-op (concurrent submitters all set the same value);
    // a different set is rejected - the client may already be built from the first set, so silently
    // replacing them would be misleading.
    if (_credentials.value() != credentials)
    {
        LOG(ERROR) << "Credentials were already set to a different value; create a new streamer to use different credentials";
        return common::ResponseCode::CredentialsAlreadySet;
    }

    return common::ResponseCode::Success;
}

common::s3::Credentials Streamer::CredentialsState::get() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    return _credentials.value_or(common::s3::Credentials{});
}

common::ResponseCode Streamer::set_credentials(const common::s3::Credentials & credentials)
{
    return _credentials_state->set(credentials);
}

common::s3::Credentials Streamer::credentials() const
{
    return _credentials_state->get();
}

common::Response Streamer::response(unsigned timeout_ms, bool & submission_done)
{
    submission_done = false;

    // Persistent + timed pop: a drained-but-open responder is not terminal (it blocks / times
    // out); FinishedError comes only from teardown. Completion is per-submission, tracked below.
    auto r = _responder->pop(timeout_ms);

    if (r.ret != common::ResponseCode::FinishedError && r.ret != common::ResponseCode::TimedOut)
    {
        submission_done = consume_submission_response(r.submission_id);
    }

    return r;
}

common::ResponseCode Streamer::async_request(
    std::vector<FileRanges> & request,
    SubmissionId * out_submission_id)
{
    // Default the caller's id to 0 ("none"). It is overwritten with the real id the instant one is
    // minted (below), so only a failure before that point (verify / plugin lock) reports no id.
    if (out_submission_id != nullptr)
    {
        *out_submission_id = 0;
    }

    // verify input
    verify_requests(request);

    // A streamer serves a single object-storage plugin; reject a submission that mixes object-storage plugins
    // or differs from the locked plugin. Nothing is committed yet, so returning is clean. Credentials are
    // streamer-scoped and read only at client creation (in the worker), so they are not touched here.
    if (const auto ret = lock_object_plugin(request); ret != common::ResponseCode::Success)
    {
        return ret;
    }

    // One response is issued per range whatever its size - a zero-sized range is completed immediately
    // below rather than reaching storage - so total_sizes counts every range.
    unsigned total_sizes = 0;
    size_t total_bytes = 0;
    for (const auto & file : request)
    {
        total_sizes += file.ranges.size();
        for (const auto & range : file.ranges)
        {
            total_bytes += range.size;
        }
    }

    // Mint the submission id up front so batches can be stamped with it, and hand it back to the
    // caller immediately: the id is always reported once it exists, regardless of how the call
    // ends. On success it identifies the submission; on a failure AFTER commit (a dispatch throw
    // under memory pressure) it lets the caller drain this submission's already-accounted
    // responses; on a pre-commit build failure it is informational (the caller sees the error and
    // will not wait on it). The submission is committed (registered + increment + dispatched) only
    // once fully built, so a build failure below just returns the error - no registry entry, no
    // increment, no workloads, and no shared cancel() that would disturb other submissions.
    const SubmissionId submission_id = _submissions.generate();
    if (out_submission_id != nullptr)
    {
        *out_submission_id = submission_id;
    }

    // A submission with no ranges at all owes no responses, so it is deliberately NOT registered:
    // completion is driven by consuming responses, and _submissions.add() with an expected count of
    // zero would insert an entry that consume() can never erase. Returning Success with the minted id
    // leaves nothing behind. (A submission whose ranges are all ZERO SIZED is different - it does owe
    // one response per range, and goes through the normal path below.)
    if (total_sizes == 0)
    {
        LOG(DEBUG) << "Submission " << submission_id << " contains no ranges; nothing to read";
        return common::ResponseCode::Success;
    }

    // divide reading between workers
    Assigner assigner(request, _config);

    std::vector<Workload> workloads(assigner.num_workloads());

    // Create batches for each contiguous transfer. Batches is built per transfer rather than per file:
    // within a transfer the ranges tile one contiguous span of both file and destination, which is the
    // assumption Batch is built on. A file whose ranges are not all adjacent yields several transfers.

    unsigned params_file_index = 0;
    bool has_params = false;
    common::s3::S3ClientWrapper::Params params;

    for (const auto & transfer : assigner.transfers())
    {
        const auto & path = request[transfer.file_index].path;

        // Transfers are produced grouped by file, so the params are rebuilt only when the file changes;
        // handle_s3 parses the URI, which would otherwise be repeated for every transfer of a file.
        if (!has_params || transfer.file_index != params_file_index)
        {
            params = handle_s3(transfer.file_index, path);
            params_file_index = transfer.file_index;
            has_params = true;
        }

        LOG(DEBUG) << "Submission " << submission_id << " creating batches for file index " << transfer.file_index
                   << " path: " << path << " offset " << transfer.offset << " size " << transfer.size
                   << " ranges " << transfer.range_sizes.size() << " from index " << transfer.first_range_index;

        Batches batches(submission_id, transfer.file_index, transfer.tasks, _config, _responder, path, params,
                        transfer.range_sizes, transfer.first_range_index);
        const auto num_batches = batches.size();
        LOG(DEBUG) << "Created " << num_batches << " batches for file index " << transfer.file_index;
        for (size_t j = 0; j < num_batches; ++j)
        {
            auto & batch = batches[j];
            if (batch.tasks.size() == 0)
            {
                LOG(WARNING) << "Found empty batch " << batch;
                continue;
            }

            const auto workload_index = batch.workload_index;

            LOG(DEBUG) << "Submission " << submission_id << " Batch: file index " << batch.file_index << " with " << batch.tasks.size() << " tasks for workload " << workload_index << " total bytes " << batch.total_bytes();

            const auto & result = workloads[workload_index].add_batch(std::move(batch));
            LOG(DEBUG) << "Submission " << submission_id << " added batch to workload " << workload_index << " with result " << result;
            if (result != common::ResponseCode::Success)
            {
                LOG(ERROR) << "Submission " << submission_id << " failed to add batch to workload " << workload_index << " error: " << result;
                return result; // nothing committed yet
            }
        }
    }

    // Commit the submission: register it, grow the persistent responder's expected count, then
    // dispatch. increment() must happen before any workload runs so _running covers the responses.
    _submissions.add(submission_id, total_sizes, total_bytes);
    _responder->increment(total_sizes);

    // The drain guard relies on push_back's strong guarantee for the workload whose push throws,
    // which holds only because Workload's move is noexcept (so the throw is the node allocation,
    // before the move). Enforce it so a future throwing-move member fails the build here.
    static_assert(std::is_nothrow_move_constructible<Workload>::value,
                  "Workload move must be noexcept for the async_request dispatch drain to be safe");

    // If dispatch throws (e.g. bad_alloc) after increment(), drain the not-yet-dispatched
    // workloads (index >= next) as UnknownError on unwind, so every sub-range still completes and
    // the responder/registry reach zero - the consumer gets a clean failure instead of hanging.
    // The exception then propagates and the caller maps it to UnknownError.
    size_t next = 0;
    utils::ScopeGuard drain_guard([&]() { drain_undispatched(submission_id, workloads, next); });

    for (; next < workloads.size(); ++next) // ++next runs after the body, so a throwing push leaves next at it
    {
        if (workloads[next].size() > 0)
        {
            LOG(DEBUG) << "Submission " << submission_id << " sending workload to worker with batches " << workloads[next].size();

            // route to the pool for this workload's backend kind (a workload is homogeneous)
            const auto kind = workloads[next].is_object_storage()
                ? BackendPools::Kind::ObjectStorage
                : BackendPools::Kind::FileSystem;
            _pools.push(kind, std::move(workloads[next]));
        }
    }

    drain_guard.cancel(); // all workloads dispatched

    return common::ResponseCode::Success;
}

void Streamer::drain_undispatched(SubmissionId submission_id, std::vector<Workload> & workloads, size_t from)
{
    LOG(ERROR) << "Submission " << submission_id << " failed to dispatch; draining undispatched workloads as errors";

    // Only workloads[from .. end] - the failed one and the un-attempted ones. workloads[0 .. from)
    // were already moved into the pool and are intentionally not referenced.
    for (size_t i = from; i < workloads.size(); ++i)
    {
        if (workloads[i].size() > 0)
        {
            try { workloads[i].fail(common::ResponseCode::UnknownError); }
            catch (...) {} // best-effort under severe OOM
        }
    }
}

bool Streamer::consume_submission_response(SubmissionId submission_id)
{
    // SubmissionsMgr owns the registry + its mutex (a strict leaf); logging happens here, outside
    // that lock. Consuming a response for an unknown submission is an accounting bug and ASSERTs
    // inside consume() rather than returning.
    const auto result = _submissions.consume(submission_id);

    if (result.outcome == SubmissionsMgr::Result::Outcome::Completed)
    {
        LOG(INFO) << "Submission " << submission_id << " completed: " << utils::logging::human_readable_size(result.total_bytes)
                  << " in " << result.elapsed_ms << " ms, " << utils::logging::human_readable_size(result.throughput_bps) << "/s";
        return true;
    }

    return false;
}

// Every destination is now caller-supplied and really dereferenced, so unlike the previous API - where only
// dsts[0] was ever used, and so only it could be checked - every range's destination is validated here.
//
// Deliberately NOT rejected:
//   - a file with no ranges: it contributes no responses and never reaches storage
//   - a range of size zero: it is completed as Success immediately (see async_request), so the caller
//     still receives exactly one response per range
//
// Overlapping destinations are NOT verified: laying out the destination buffer is the caller's
// responsibility. Such a check is possible here if it is ever wanted - sort the ranges by destination and
// compare each against its neighbour, O(n log n) per submission.
void Streamer::verify_requests(std::vector<FileRanges> & request)
{
    for (const auto & file : request)
    {
        LOG(SPAM) << "Requested to read asynchronously " << file.ranges.size() << " ranges from " << file.path;

        for (const auto & range : file.ranges)
        {
            // a zero-sized range writes nothing, so a null destination is harmless there
            if (range.size > 0 && range.dst == nullptr)
            {
                LOG(ERROR) << "Destination buffer is null for " << file.path << " offset " << range.offset;
                throw common::Exception(common::ResponseCode::InvalidParameterError);
            }
        }
    }
}

common::ResponseCode Streamer::lock_object_plugin(const std::vector<FileRanges> & request)
{
    // Classify every path, and reject a submission that mixes backends - either two object-storage
    // plugins, or filesystem and object storage together.
    //
    // A STREAMER serves both kinds happily: BackendPools holds one pool per kind, created lazily, so a
    // filesystem submission and an object-storage submission can follow each other on the same streamer.
    // A SUBMISSION must pick one, because the Assigner divides it with a single backend's worker count
    // and block size, and a workload must be homogeneous for BackendPools::push to route it. Without
    // this check a mixed submission is accepted or rejected depending on where the assigner's slice
    // happens to land - InvalidParameterError out of Workload::add_batch when both kinds share a
    // workload, silently accepted with the wrong block size when they do not.
    std::optional<BackendPools::Plugin> submission_plugin;
    bool has_filesystem = false;
    for (const auto & file : request)
    {
        // A file with no ranges reaches no storage at all (verify_requests accepts it deliberately, and it
        // yields no transfer), so it must not influence backend selection. Classifying it would let an
        // empty "s3://..." entry permanently lock the streamer's plugin - and build the object-storage
        // pool - for a submission that reads nothing, and would reject a filesystem submission that merely
        // carries an empty object-storage entry alongside it.
        if (file.ranges.empty())
        {
            continue;
        }

        auto uri = try_parse_uri(file.path);
        if (uri == nullptr)
        {
            has_filesystem = true;
            continue;   // filesystem path
        }

        const auto plugin = uri->is_gcs()   ? BackendPools::Plugin::GCS
                          : uri->is_azure() ? BackendPools::Plugin::Azure
                          :                   BackendPools::Plugin::S3;

        if (submission_plugin.has_value() && submission_plugin.value() != plugin)
        {
            LOG(ERROR) << "Submission mixes object storage plugins; rejecting";
            return common::ResponseCode::UnsupportedBackendMix;
        }
        submission_plugin = plugin;
    }

    if (!submission_plugin.has_value())
    {
        return common::ResponseCode::Success;   // pure filesystem submission - nothing to lock
    }

    if (has_filesystem)
    {
        LOG(ERROR) << "Submission mixes filesystem and object storage paths; rejecting";
        return common::ResponseCode::UnsupportedBackendMix;
    }

    // Lock the object-storage pool to this plugin (first submission) or verify it matches; the lock lives in
    // BackendPools, alongside the ObjectStorage pool it constrains
    return _pools.lock_object_plugin(submission_plugin.value());
}

std::shared_ptr<common::s3::StorageUri> Streamer::try_parse_uri(const std::string & path)
{
    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_shared<common::s3::StorageUri>(path);
    }
    catch(const std::exception& e)
    {
    }
    return uri;
}

common::s3::S3ClientWrapper::Params Streamer::handle_s3(unsigned file_index, const std::string & path)
{
    auto uri = try_parse_uri(path);

    if (uri != nullptr)
    {
        // streaming-only setup (fd limit + stop), once. If the fd-limit raise throws
        // (InsufficientFdLimit) the flag stays unset, so a later submission retries.
        std::call_once(_s3_stream_init_flag, [this]()
        {
            // adjust fd limit acording to concurrency
            auto fd_limit = utils::get_cur_file_descriptors();
            LOG(DEBUG) << "Process file descriptors limit is " << fd_limit << " and concurrency level is " << _config->s3_concurrency;
            const auto desired_fd_limit = _config->s3_concurrency * 64;
            if (fd_limit < desired_fd_limit)
            {
                if (desired_fd_limit > utils::get_max_file_descriptors())
                {
                    LOG(ERROR) << "Insufficient file descriptors limit " << fd_limit << " for concurrency level " << _config->s3_concurrency << " ; increase fd limit to " << desired_fd_limit << " or higher, depending on your application fd usage";
                    throw common::Exception(common::ResponseCode::InsufficientFdLimit);
                }
                LOG(INFO) << "Increasing fd soft limit to " << desired_fd_limit << " for concurrency level " << _config->s3_concurrency;
                _fd_limit = std::make_unique<utils::FdLimitSetter>(desired_fd_limit);
            }
            _s3_stop = std::make_unique<S3Stop>();
        });

        // S3Cleanup: shared by list_files and streaming, created once
        std::call_once(_s3_cleanup_init_flag, [this]() { _s3 = std::make_unique<S3Cleanup>(); });
    }

    // Batch params carry only the URI (used by the per-read path) - no credentials. Credentials are applied
    // once, at client creation, from the streamer's credentials() (see ObjectStorageWorker::capacity).
    return common::s3::S3ClientWrapper::Params(uri, _config->s3_block_bytesize);
}

std::vector<std::pair<std::string, size_t>> Streamer::list_files(
    const std::string & prefix,
    bool is_recursive,
    const std::vector<std::string> & allow_patterns,
    const std::vector<std::string> & ignore_patterns)
{
    // fnmatch(3) filtering, matching the behavior of Python fnmatch.fnmatch(path, pattern)
    auto keep = [&](const char * path) -> bool
    {
        if (!allow_patterns.empty())
        {
            bool matched = false;
            for (const auto & p : allow_patterns)
            {
                if (fnmatch(p.c_str(), path, 0) == 0) { matched = true; break; }
            }
            if (!matched) return false;
        }
        for (const auto & p : ignore_patterns)
        {
            if (fnmatch(p.c_str(), path, 0) == 0) return false;
        }
        return true;
    };

    std::vector<std::pair<std::string, size_t>> results;

    auto uri = try_parse_uri(prefix);

    if (uri != nullptr)
    {
        // Any code path that uses the S3 plugin must release its clients and the backend handle
        // when the streamer shuts down (~Streamer). stop() and the fd limit are streaming-only:
        // listing is a single synchronous call with no in-flight async reads and no threadpool
        // workers to unblock. Shares the S3Cleanup once_flag with handle_s3 so it is created once.
        std::call_once(_s3_cleanup_init_flag, [this]() { _s3 = std::make_unique<S3Cleanup>(); });

        // listing builds a client, so read the streamer's credentials here (a client-creation point)
        common::s3::S3ClientWrapper::Params params(uri, credentials(), _config->s3_block_bytesize);
        common::s3::S3ClientWrapper wrapper(params);

        common::backend_api::ObjectFileEntry_t * entries = nullptr;
        unsigned num_entries = 0;
        auto ret = wrapper.list_files(prefix.c_str(), is_recursive ? 1 : 0, &entries, &num_entries);
        if (ret != common::ResponseCode::Success)
        {
            throw common::Exception(ret); // entries is not allocated on error
        }

        // free_file_list is a no-op for nullptr (empty listing); ScopeGuard is
        // constructed only on the success path so it never fires on error.
        utils::ScopeGuard guard([&]{ wrapper.free_file_list(entries, num_entries); });

        for (unsigned i = 0; i < num_entries; ++i)
        {
            if (keep(entries[i].path))
            {
                results.emplace_back(entries[i].path, entries[i].size);
            }
        }
    }
    else
    {
        namespace fs = std::filesystem;
        const fs::path root(prefix);
        if (!fs::exists(root))
        {
            throw common::Exception(common::ResponseCode::FileAccessError);
        }

        auto process = [&](const fs::directory_entry & entry)
        {
            if (entry.is_regular_file() && keep(entry.path().c_str()))
            {
                results.emplace_back(entry.path().string(), static_cast<size_t>(entry.file_size()));
            }
        };

        if (is_recursive)
        {
            for (const auto & e : fs::recursive_directory_iterator(root)) process(e);
        }
        else
        {
            for (const auto & e : fs::directory_iterator(root)) process(e);
        }
    }

    return results;
}

}; // namespace runai::llm::streamer::impl
