#include "streamer/impl/streamer/streamer.h"

#include <fnmatch.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <limits>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "utils/logging/logging.h"
#include "utils/scope_guard/scope_guard.h"

#include "streamer/impl/workload/workload.h"
#include "streamer/impl/async_io/async_io_worker/async_io_worker.h"
#include "streamer/impl/object_storage_worker/object_storage_worker.h"
#include "streamer/impl/assigner/assigner.h"
#include "streamer/impl/batches/batches.h"
#include "common/exception/exception.h"
#include "posix_io/alignment/alignment.h"
#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::impl
{

Streamer::Streamer() : Streamer(Config())
{}

Streamer::Streamer(Config config, Environment environment) :
    _config(std::make_shared<Config>(config)),
    // Built here, resolved on the first submission - so RUNAI_STREAMER_FS_STRATEGY is only the
    // DEFAULT, and anything set between runai_start() and the first request still takes effect.
    _strategy_resolver(std::make_shared<StrategyResolver>(config.fs_strategy_candidates,
                                                          environment.availability)),
    // Filesystem reads are synchronous (concurrency threads, stateless handler); object-storage reads are
    // asynchronous (s3_concurrency ObjectStorageWorkers, each owning a client + in-flight capacity window).
    // Pools are created lazily on first use of each kind.
    _pools(
        [](Workload&& workload, std::atomic<bool> & stopped)
        {
            workload.execute(stopped);
        },
        // the async worker owns an IoEngine built for the resolved strategy. Reading the strategy
        // here is safe: this factory runs when the pool is created, which is the first push, which is
        // after resolution. Captures the resolver by value, never `this`.
        [resolver = _strategy_resolver, workers = _async_workers]() -> std::unique_ptr<utils::Worker<Workload>>
        {
            auto worker = std::make_unique<AsyncIoWorker>(resolver->resolved());

            // Registered here, where the concrete type is still known. The pool stores it as a
            // Worker<Workload>, which knows nothing of counters, so this is the last point at which
            // it can be recorded without a cast.
            workers->add(worker.get());
            return worker;
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
    _responder(std::make_shared<common::Responder>(0, common::QueueMode::PERSISTENT)),
    _environment(std::move(environment))
{
    LOG(DEBUG) << config;
}

Streamer::~Streamer()
{
    try
    {
        // At INFO because it is the only way to tell a real direct read from one that bounced every
        // pass through the scratch buffer. Both reach the same bytes and both look identical in the
        // log otherwise, so without this a run that lost congruence reads as a working direct run.
        //
        // Reported here, at the end, because the counters are totals over the streamer's life: a
        // ratio taken mid-run would only describe the submissions seen so far.
        //
        // Silent when nothing was read asynchronously, so a synchronous run does not gain a line of
        // zeroes that means nothing.
        const auto counters = async_counters();
        if (counters.bytes_read != 0)
        {
            LOG(INFO) << "Async io totals: " << counters;
        }

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

const AsyncIoStats & Streamer::stats() const
{
    return _stats;
}

AsyncIoCounters Streamer::async_counters() const
{
    return _async_workers->total();
}

unsigned Streamer::async_engines() const
{
    return _pools.async_engines();
}

common::ResponseCode Streamer::set_fs_strategy(const std::string & candidates)
{
    return _strategy_resolver->set_candidates(candidates);
}

posix_io::Strategy Streamer::fs_strategy() const
{
    return _strategy_resolver->resolved();
}

bool Streamer::async_pool_used() const
{
    return _pools.async_pool_used();
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

    // Settle which filesystem strategy this streamer uses - once, here, for the same reasons as the
    // plugin lock above: nothing is committed yet, so returning is clean, and every setter has had
    // its chance to run. Idempotent, so every later submission takes a fast path through it.
    //
    // ONLY for a filesystem submission. The strategy names a filesystem engine and has nothing to say
    // about object storage, so resolving it here would let an unservable filesystem strategy reject an
    // S3 read - failing a submission for a reason that cannot apply to it. It also means a streamer
    // that only ever touches object storage never probes io_uring at all.
    const bool object_storage = is_object_storage_submission(request);

    if (!object_storage)
    {
        if (const auto ret = _strategy_resolver->resolve(); ret != common::ResponseCode::Success)
        {
            return ret;
        }
    }

    // One response per range whatever its size, so total_ranges counts every range - a zero-sized one is
    // completed below without reaching storage. A COUNT, unlike total_bytes beside it.
    size_t total_ranges = 0;
    size_t total_bytes = 0;
    for (const auto & file : request)
    {
        total_ranges += file.ranges.size();
        for (const auto & range : file.ranges)
        {
            total_bytes += range.size;
        }
    }

    // The responder and the submission registry both carry the expected count as `unsigned`, so a total
    // that does not fit would truncate to total_ranges % 2^32 - fewer responses expected than ranges
    // submitted, so the submission reports done early and the caller frees buffers still being written.
    // Rejected before an id is minted, rather than narrowed silently.
    //
    // Untested on purpose: unreachable below ~4.29 billion ranges in one submission.
    if (total_ranges > std::numeric_limits<unsigned>::max())
    {
        LOG(ERROR) << "Submission has " << total_ranges << " ranges, which exceeds the maximum of "
                   << std::numeric_limits<unsigned>::max();
        return common::ResponseCode::InvalidParameterError;
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
    if (total_ranges == 0)
    {
        LOG(DEBUG) << "Submission " << submission_id << " contains no ranges; nothing to read";
        return common::ResponseCode::Success;
    }

    // divide reading between workers
    // Object storage has no mount to probe and no strategy to consult, so it is not asked.
    //
    // The devices are kept alongside: a workload's group is an index into this, and the st_dev it
    // names is the key its engine is chosen by.
    std::vector<dev_t> group_devices;
    const std::vector<int> group_by_file = object_storage ? std::vector<int>{}
                                                          : file_groups(request, group_devices);
    Assigner assigner(request, _config, group_by_file);

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
    // narrowing is safe: the guard above rejected anything that does not fit
    const auto expected_responses = static_cast<unsigned>(total_ranges);
    _submissions.add(submission_id, expected_responses, total_bytes);
    _responder->increment(expected_responses);

    // The drain guard relies on push_back's strong guarantee for the workload whose push throws,
    // which holds only because Workload's move is noexcept (so the throw is the node allocation,
    // before the move). Enforce it so a future throwing-move member fails the build here.
    static_assert(std::is_nothrow_move_constructible<Workload>::value,
                  "Workload move must be noexcept for the async_request dispatch drain to be safe");

    // If dispatch throws (e.g. bad_alloc) after increment(), drain the not-yet-dispatched
    // workloads (index >= next) as UnknownError on unwind, so every sub-range still completes and
    // the responder/registry reach zero - the consumer gets a clean failure instead of hanging.
    size_t next = 0;
    utils::ScopeGuard drain_guard([&]() { drain_undispatched(submission_id, workloads, next); });

    try
    {
        for (; next < workloads.size(); ++next) // ++next runs after the body, so a throwing push leaves next at it
        {
            if (workloads[next].size() > 0)
            {
                LOG(DEBUG) << "Submission " << submission_id << " sending workload to worker with batches " << workloads[next].size();

                // Route to this workload's pool - a workload is homogeneous.
                //
                // The filesystem choice is the strategy resolved above, which is streamer-scoped, so
                // The pool was decided per file before the batches were built, and nothing moves
                // between pools afterwards - so this only reads the answer.
                const int group = workloads[next].is_object_storage() ? -1 : assigner.group_of_workload(next);

                if (workloads[next].is_object_storage())
                {
                    _pools.push(BackendPools::Pool::ObjectStorage, std::move(workloads[next]));
                }
                else if (group < 0)
                {
                    _pools.push(BackendPools::Pool::FileSystem, std::move(workloads[next]));
                }
                else
                {
                    // The mount picks the engine. Every task in this workload is on one mount, which
                    // is why the group exists - see Assigner's group_by_file.
                    ASSERT(static_cast<size_t>(group) < group_devices.size())
                        << "async workload " << next << " has group " << group
                        << " but only " << group_devices.size() << " mounts were probed";

                    _pools.push_async(group_devices[group], std::move(workloads[next]));
                }
            }
        }
    }
    catch (...)
    {
        // A failure HERE is past the point of no return: the submission is registered and its responses
        // are already counted. There is no recovery - report UnknownError, whatever was thrown.
        //
        // UnknownError is the code that tells the caller to abort everything; every other code says the
        // failure is attributable to this submission and the caller may carry on. Reporting a specific
        // code from here would be a lie, because drain_undispatched (still armed - it runs as this
        // returns) fails the undispatched ranges as UnknownError, and because the drain itself is
        // best-effort under severe OOM, so the submission-done flag may never arrive. Normalising here
        // rather than relying on the C layer's catch-all keeps that true whatever a future change throws:
        // today only std::bad_alloc can escape, which the catch-all would have mapped correctly by
        // accident, but a common::Exception would now surface its own code instead.
        LOG(ERROR) << "Submission " << submission_id << " failed during dispatch; reporting UnknownError";
        return common::ResponseCode::UnknownError;
    }

    drain_guard.cancel(); // all workloads dispatched

    // Recorded only once every workload is dispatched. A submission that failed before this point was
    // never read by anything, so recording it would say a run happened that did not.
    {
        SubmissionStats stats;
        stats.submission_id = submission_id;
        stats.shared_engine_mounts = _pools.shared_engine_mounts();

        // Object-storage files are left out rather than labelled: the strategy names a filesystem
        // reader, and none of it applies to them.
        if (!object_storage)
        {
            stats.files.reserve(request.size());
            for (size_t i = 0; i < request.size(); ++i)
            {
                const int group = i < group_by_file.size() ? group_by_file[i] : -1;
                stats.files.push_back({ request[i].path,
                                        group < 0 ? posix_io::Strategy::SyncBuffered
                                                  : _strategy_resolver->resolved() });
            }
        }

        _stats.record(stats);
    }

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

bool Streamer::is_object_storage_submission(const std::vector<FileRanges> & request)
{
    for (const auto & file : request)
    {
        // A file with no ranges reaches no storage, so it must not decide the backend - the same rule
        // lock_object_plugin and Assigner::check_object_storage already follow.
        if (file.ranges.empty())
        {
            continue;
        }

        return try_parse_uri(file.path) != nullptr;
    }

    return false;
}

std::vector<int> Streamer::file_groups(const std::vector<FileRanges> & request,
                                       std::vector<dev_t> & out_devices)
{
    std::vector<int> group_by_file(request.size(), -1);
    out_devices.clear();

    if (!posix_io::is_async(_strategy_resolver->resolved()))
    {
        return group_by_file;   // the synchronous reader serves everything
    }


    // Directory -> its group. MountCapabilities caches by st_dev, which saves the statfs but NOT the
    // stat that finds st_dev in the first place - so without this a 200-shard model in one directory
    // would stat that directory 200 times. Per submission, because it is only read inside this loop.
    std::map<std::string, int> by_directory;

    // st_dev -> group id, so two directories on the same mount share a group and therefore an engine.
    std::map<dev_t, int> by_device;

    // Which group serves this directory, or -1 for the synchronous reader. Answered once per
    // directory; every shard beside the first is free.
    const auto group_of_directory = [&](const std::string & directory, const std::string & path)
    {
        const auto seen = by_directory.find(directory);
        if (seen != by_directory.end())
        {
            return seen->second;
        }

        int group = -1;
        try
        {
            const auto capability = _environment.mount ? _environment.mount(directory)
                                                       : _mounts.of_path(directory);

            // tmpfs and ramfs are pure memcpy with no device to overlap, so depth buys nothing and
            // parallelism does - the 16-thread pool is the right reader for them (5.12).
            if (!capability.memory_backed)
            {
                // One group per MOUNT, so directories sharing a mount share an engine. Groups are
                // numbered in first-seen order, which is what makes them dense indices into
                // out_devices.
                const auto device = by_device.find(capability.dev);
                if (device != by_device.end())
                {
                    group = device->second;
                }
                else
                {
                    group = static_cast<int>(out_devices.size());
                    by_device.emplace(capability.dev, group);
                    out_devices.push_back(capability.dev);
                }
            }
        }
        catch (const common::Exception & e)
        {
            // Deliberately NOT fatal. A directory we cannot stat means we cannot tell what serves it
            // best, not that the read must fail - and failing here would turn a per-file problem into
            // a whole-submission one, which is exactly what the missing file itself will report
            // later, attributably.
            LOG(WARNING) << "Cannot probe the mount of " << directory << " (" << e.error()
                         << "); reading " << path << " with the synchronous reader";
        }

        // Remembered whatever the answer, so an unprobeable directory is not retried once per file.
        by_directory.emplace(directory, group);
        return group;
    };

    // libaio has no asynchronous buffered mode, so a file it cannot read directly must be routed away
    // before dispatch. Every other async strategy keeps its files whatever this would have said - see
    // reads_directly().
    const bool check_direct = _strategy_resolver->resolved() == posix_io::Strategy::LibaioDirect;

    for (size_t i = 0; i < request.size(); ++i)
    {
        const auto & path = request[i].path;

        // A file with no ranges reaches no storage, so probing its mount would be a syscall for
        // nothing - and would fail the probe on a path that was never going to be read.
        if (request[i].ranges.empty())
        {
            continue;
        }

        // The DIRECTORY, not the file: capability belongs to the mount, so the answer is the same for
        // every shard beside it. It also works for a file that does not exist yet, where stat'ing the
        // file itself would fail.
        const auto slash = path.find_last_of('/');
        const std::string directory = (slash == std::string::npos) ? std::string(".")
                                    : (slash == 0 ? std::string("/") : path.substr(0, slash));

        const int group = group_of_directory(directory, path);
        if (group < 0)
        {
            continue;   // the synchronous reader serves it
        }

        // PER FILE, so it cannot be answered from the directory cache above: congruence depends on
        // this file's own offsets and destinations, and two shards in one directory can differ.
        if (check_direct && !reads_directly(request[i], out_devices[group]))
        {
            continue;
        }

        group_by_file[i] = group;
    }

    return group_by_file;
}

bool Streamer::reads_directly(const FileRanges & file, dev_t device)
{
    const auto block = posix_io::DirectBlockSize;

    for (const auto & range : file.ranges)
    {
        // A zero-sized range produces no chunk, so it is never read and never opens the file. Letting
        // one decide the reader for the whole file would be deciding on a range nobody reads.
        if (range.size == 0)
        {
            continue;
        }

        if (!posix_io::is_congruent(range.offset, range.dst, block))
        {
            // No PART of this range can be read directly - not the middle, not one block of it. So
            // the worker would open the file buffered, and under libaio that is a serial read.
            LOG(DEBUG) << "Reading " << file.path << " with the synchronous reader: offset "
                       << range.offset << " and its destination are not congruent for block " << block;
            return false;
        }
    }

    const auto support = _environment.direct ? _environment.direct(device, file.path)
                                             : _mounts.direct_support(device, file.path);

    if (support == posix_io::DirectSupport::No)
    {
        LOG(DEBUG) << "Reading " << file.path << " with the synchronous reader: its mount cannot"
                   << " serve O_DIRECT, and libaio without O_DIRECT reads one file at a time";
        return false;
    }

    return true;
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
