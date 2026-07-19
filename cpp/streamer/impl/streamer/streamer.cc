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
        []() -> std::unique_ptr<utils::Worker<Workload>>
        {
            return std::make_unique<ObjectStorageWorker>();
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

common::ResponseCode Streamer::sync_read(const std::string & path, size_t file_offset, size_t bytesize, void * dst, const common::s3::Credentials & credentials)
{
    LOG(SPAM) << "Requested to read " << bytesize << " bytes from " << path << " offset " << file_offset;

    auto r = async_read(path, file_offset, bytesize, dst, 1, &bytesize, credentials);
    if (r != common::ResponseCode::Success)
    {
        return r;
    }

    // route through response() so the submission's registry record is consumed/forgotten
    return response().ret;
}

common::ResponseCode Streamer::async_read(const std::string & path, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes, const common::s3::Credentials & credentials)
{
    common::ResponseCode ret = common::ResponseCode::Success;

    try
    {
        std::vector<std::string> paths;
        std::vector<size_t> file_offsets;
        std::vector<size_t> bytesizes;
        std::vector<void *> dsts;
        std::vector<unsigned> num_sizes_v;
        std::vector<std::vector<size_t>> internal_sizes_vv;

        paths.push_back(path);
        file_offsets.push_back(file_offset);
        bytesizes.push_back(bytesize);
        dsts.push_back(dst);
        num_sizes_v.push_back(num_sizes);

        std::vector<size_t> internal_sizes_v(internal_sizes, internal_sizes + num_sizes);

        internal_sizes_vv.push_back(internal_sizes_v);

        ret = async_request(paths, file_offsets, bytesizes, dsts, num_sizes_v, internal_sizes_vv, credentials);
    }
    catch(const common::Exception & e)
    {
        LOG(ERROR) << "caught exception " << e.what();
        ret = e.error();
    }

    return ret;
}

bool Streamer::busy() const
{
    // not drained: some submission still has responses in flight or waiting to be consumed
    return _responder != nullptr && !_responder->finished();
}

common::Response Streamer::response()
{
    if (_responder == nullptr)
    {
        return common::Response(common::ResponseCode::FinishedError);
    }

    // Legacy finish-on-drain, kept at the streamer level: the persistent responder never
    // self-finishes, so when nothing is outstanding (all pushed and popped) we report the
    // historical FinishedError rather than blocking for a future submission.
    if (_responder->finished())
    {
        return common::Response(common::ResponseCode::FinishedError);
    }

    auto r = _responder->pop();

    // account for the consumed response (per-submission throughput + reclamation). The legacy
    // path ignores whether this was the submission's last response; runai_response_ex uses it.
    if (r.ret != common::ResponseCode::FinishedError && r.ret != common::ResponseCode::TimedOut)
    {
        consume_submission_response(r.submission_id);
    }

    return r;
}

common::Response Streamer::response_ex(unsigned timeout_ms, bool & submission_done)
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
    std::vector<std::string> & paths,
    std::vector<size_t> & file_offsets,
    std::vector<size_t> & bytesizes,
    std::vector<void *> & dsts,
    std::vector<unsigned> & num_sizes,
    std::vector<std::vector<size_t>> & internal_sizes,
    const common::s3::Credentials & credentials,
    unsigned * out_submission_id)
{
    // Default the caller's id to 0 ("none"). It is overwritten with the real id the instant one is
    // minted (below), so only a failure before that point (verify / plugin lock) reports no id.
    if (out_submission_id != nullptr)
    {
        *out_submission_id = 0;
    }

    // verify input
    verify_requests(paths, file_offsets, bytesizes, num_sizes, dsts);

    // A streamer serves a single object-storage plugin; reject a submission that mixes object-storage
    // plugins or uses one differing from the locked plugin. Nothing is committed yet, so returning is clean.
    if (const auto ret = lock_object_plugin(paths); ret != common::ResponseCode::Success)
    {
        return ret;
    }

    const auto total_sizes = std::accumulate(num_sizes.begin(), num_sizes.end(), 0u);
    const size_t total_bytes = std::accumulate(bytesizes.begin(), bytesizes.end(), static_cast<size_t>(0));

    // Mint the submission id up front so batches can be stamped with it, and hand it back to the
    // caller immediately: the id is always reported once it exists, regardless of how the call
    // ends. On success it identifies the submission; on a failure AFTER commit (a dispatch throw
    // under memory pressure) it lets the caller drain this submission's already-accounted
    // responses; on a pre-commit build failure it is informational (the caller sees the error and
    // will not wait on it). The submission is committed (registered + increment + dispatched) only
    // once fully built, so a build failure below just returns the error - no registry entry, no
    // increment, no workloads, and no shared cancel() that would disturb other submissions.
    const unsigned submission_id = _submissions.generate();
    if (out_submission_id != nullptr)
    {
        *out_submission_id = submission_id;
    }

    // divide reading between workers
    Assigner assigner(paths, file_offsets, bytesizes, dsts, _config);

    std::vector<Workload> workloads(assigner.num_workloads());

    // Create batches for each file

    for (size_t i = 0; i < paths.size(); ++i)
    {
        auto params = handle_s3(i, paths[i], credentials);
        LOG(DEBUG) << "Submission " << submission_id << " creating batches for file index " << i << " path: " <<  paths[i];
        Batches batches(submission_id, i, assigner.file_assignments(i), _config, _responder, paths[i], params, internal_sizes[i]);
        const auto num_batches = batches.size();
        LOG(DEBUG) << "Created " << num_batches << " batches for file index " << i;
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

void Streamer::drain_undispatched(unsigned submission_id, std::vector<Workload> & workloads, size_t from)
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

bool Streamer::consume_submission_response(unsigned submission_id)
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

void Streamer::verify_requests(std::vector<std::string> & paths, std::vector<size_t> & file_offsets, std::vector<size_t> & bytesizes, std::vector<unsigned> & num_sizes, std::vector<void *> & dsts)
{
    // Only dsts[0] is checked because only dsts[0] is ever used: for CPU reads the destination is a single
    // contiguous buffer whose base is dsts[0], and every file/sub-range is written at an offset into it (see
    // Assigner: "ASSUMES dsts[0] is base of one large buffer"). dsts[1..] are never dereferenced - callers
    // may even pass a single-element dsts for a multi-file request - so there is no per-file null to check.
    if (dsts[0] == 0)
    {
        LOG(ERROR) << "Destination buffer is null";
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }

    for (size_t i = 0; i < paths.size(); ++i)
    {
        LOG(SPAM) << "Requested to read asynchronously " << bytesizes[i] << " bytes from " << paths[i] << " offset " << file_offsets[i] << " in " << num_sizes[i] << " chunks";

        if (bytesizes[i] == 0 && num_sizes[i] == 0)
        {
            LOG(ERROR) << "Empty request - no response will be sent";
            throw common::Exception(common::ResponseCode::EmptyRequestError);
        }

        if (num_sizes[i] == 0 || bytesizes[i] == 0)
        {
            LOG(ERROR) << "Total bytes to read is " << bytesizes[i] << " but number of sub requests is " << num_sizes[i];
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
    }
}

common::ResponseCode Streamer::lock_object_plugin(const std::vector<std::string> & paths)
{
    // Find the object-storage plugin this submission uses (filesystem paths are ignored and coexist);
    // reject a submission that itself mixes two object-storage plugins.
    std::optional<BackendPools::Plugin> submission_plugin;
    for (const auto & path : paths)
    {
        auto uri = try_parse_uri(path);
        if (uri == nullptr)
        {
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

    // Lock the object-storage pool to this plugin (first submission) or verify it matches; the lock
    // lives in BackendPools, alongside the ObjectStorage pool it constrains
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

common::s3::S3ClientWrapper::Params Streamer::handle_s3(unsigned file_index, const std::string & path, const common::s3::Credentials & credentials)
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

    return common::s3::S3ClientWrapper::Params(uri, credentials, _config->s3_block_bytesize);
}

std::vector<std::pair<std::string, size_t>> Streamer::list_files(
    const std::string & prefix,
    bool is_recursive,
    const std::vector<std::string> & allow_patterns,
    const std::vector<std::string> & ignore_patterns,
    const common::s3::Credentials & credentials)
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

        common::s3::S3ClientWrapper::Params params(uri, credentials, _config->s3_block_bytesize);
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
