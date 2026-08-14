#include "streamer/impl/object_storage_worker/object_storage_worker.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "streamer/impl/assigner/assigner.h"
#include "streamer/impl/batches/batches.h"

#include "common/exception/exception.h"
#include "common/s3_wrapper/s3_wrapper.h"

#include "utils/threadpool/threadpool.h"
#include "utils/random/random.h"
#include "utils/thread/thread.h"
#include "utils/dylib/dylib.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

namespace
{

// One object-storage submission spread over several files, built the way Streamer::async_request builds it:
// one submission_id for the whole submission, one Batches per file. Bundles the config/responder/paths so a
// test can wait on the responder and assert per-file, per-request completion.
struct Submission
{
    Submission(SubmissionId submission_id,
               unsigned num_files,
               std::shared_ptr<Config> config,
               std::shared_ptr<common::Responder> responder,
               unsigned ranges_per_file = 0) :
        submission_id(submission_id),
        config(config),
        responder(responder),
        num_chunks(num_files),
        expected(num_files)
    {
        const std::string bucket = "test-bucket";
        std::vector<std::vector<size_t>> chunks(num_files);

        for (unsigned i = 0; i < num_files; ++i)
        {
            const auto size = utils::random::number(1000, 100000);
            num_chunks[i] = ranges_per_file == 0 ? utils::random::number(1, 20) : ranges_per_file;
            EXPECT_LT(num_chunks[i], size);
            responder->increment(num_chunks[i]);
            total_bytes += size;

            paths.push_back("s3://" + bucket + "/" + utils::random::string());

            // the ranges must exist before the Assigner is built - it coalesces them
            chunks[i] = utils::random::chunks(size, num_chunks[i]);
            EXPECT_EQ(chunks[i].size(), num_chunks[i]);
        }

        buffer.resize(total_bytes);

        // each file's ranges tile it contiguously, and the files are packed consecutively into the one
        // buffer, so every file coalesces to exactly one transfer
        request.resize(num_files);
        char * dst = buffer.data();
        for (unsigned i = 0; i < num_files; ++i)
        {
            request[i].path = paths[i];
            request[i].ranges.reserve(chunks[i].size());

            size_t offset = 0;
            for (const auto size : chunks[i])
            {
                request[i].ranges.push_back(ReadRange{ offset, size, dst });
                offset += size;
                dst += size;
            }
        }
    }

    // build the workloads (one Assigner over the request, Batches per contiguous transfer) ready to push
    // to the pool
    std::vector<Workload> build()
    {
        Assigner assigner(request, config);
        std::vector<Workload> workloads(assigner.num_workloads());

        const common::s3::Credentials credentials;
        for (const auto & transfer : assigner.transfers())
        {
            const auto file_idx = transfer.file_index;

            auto uri = std::make_shared<common::s3::StorageUri>(paths[file_idx]);
            common::s3::S3ClientWrapper::Params params(uri, credentials, config->s3_block_bytesize);

            Batches batches(submission_id, file_idx, transfer.tasks, config, responder, paths[file_idx], params,
                            transfer.range_sizes, transfer.first_range_index);
            for (size_t j = 0; j < batches.size(); ++j)
            {
                workloads[batches[j].workload_index].add_batch(std::move(batches[j]));
            }

            for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
            {
                expected[file_idx].insert(i);
            }
        }
        return workloads;
    }

    unsigned total_requests() const
    {
        return std::accumulate(num_chunks.begin(), num_chunks.end(), 0u);
    }

    SubmissionId submission_id;
    std::shared_ptr<Config> config;
    std::shared_ptr<common::Responder> responder;
    std::vector<std::string> paths;
    std::vector<FileRanges> request;
    std::vector<char> buffer;
    size_t total_bytes = 0;
    std::vector<unsigned> num_chunks;
    std::vector<std::set<int>> expected;
};

} // namespace

// Fixture: owns the s3 mock handle, resets its knobs before each test, and releases the plugin's clients +
// backend handle after each test (so state never leaks between tests). Provides build() to set up a
// submission, and small wrappers around the mock knobs.
class ObjectStorageWorkerTest : public ::testing::Test
{
 protected:
    ObjectStorageWorkerTest() : _dylib("libstreamers3.so") {}

    void SetUp() override
    {
        set_response_time(0);
        set_sentinel(false);
    }

    void TearDown() override
    {
        _dylib.dlsym<void(*)()>("runai_mock_s3_cleanup")();
        common::s3::S3ClientWrapper::shutdown();
    }

    // create a Config/Responder and a (num_files) submission; returns the workloads ready to dispatch. Stores
    // the config/responder/submission as members so the test can build a pool and wait on the responder.
    std::vector<Workload> build(unsigned num_files, unsigned s3_concurrency, unsigned ranges_per_file = 0)
    {
        make_context(s3_concurrency);
        submission = std::make_unique<Submission>(utils::random::number(), num_files, config, responder, ranges_per_file);
        return submission->build();
    }

    // set up config + responder without a submission (for tests that build several submissions themselves)
    void make_context(unsigned s3_concurrency)
    {
        // (concurrency, s3_concurrency, fs_block, s3_block, force_min_chunk=false)
        config = std::make_shared<Config>(s3_concurrency, s3_concurrency, utils::random::number<size_t>(1, 1024), utils::random::number<size_t>(1, 1024), false);
        responder = std::make_shared<common::Responder>(0);
    }

    static utils::ThreadPool<Workload> make_pool(unsigned size)
    {
        return utils::ThreadPool<Workload>(
            []() -> std::unique_ptr<utils::Worker<Workload>>
            {
                // the s3 mock client ignores credentials, so the provider returns an empty set
                return std::make_unique<ObjectStorageWorker>([]() { return common::s3::Credentials{}; });
            },
            size);
    }

    static void push_all(utils::ThreadPool<Workload> & pool, std::vector<Workload> & workloads)
    {
        for (auto & workload : workloads)
        {
            if (workload.size() > 0)
            {
                pool.push(std::move(workload));
            }
        }
    }

    void set_response_time(unsigned ms) { _dylib.dlsym<void(*)(unsigned)>("runai_mock_s3_set_response_time_ms")(ms); }
    void set_sentinel(bool on)          { _dylib.dlsym<void(*)(bool)>("runai_mock_s3_set_append_finished_sentinel")(on); }
    void set_window(size_t bytes)       { _dylib.dlsym<void(*)(size_t)>("runai_mock_s3_set_inflight_window")(bytes); }
    void set_read_failures(unsigned count, common::ResponseCode code)
    {
        _dylib.dlsym<void(*)(unsigned, common::backend_api::ResponseCode_t)>("runai_mock_s3_set_read_failures")(count, code);
    }
    size_t total_read_requests()        { return _dylib.dlsym<size_t(*)()>("runai_mock_s3_total_read_requests")(); }
    size_t max_concurrent()             { return _dylib.dlsym<size_t(*)()>("runai_mock_s3_max_concurrent")(); }
    int clients()                       { return _dylib.dlsym<int(*)()>("runai_mock_s3_clients")(); }

    static size_t count_object_chunks(const std::vector<Workload> & workloads, size_t chunk_bytesize)
    {
        size_t result = 0;
        for (const auto & workload : workloads)
        {
            for (const auto & batch : workload.batches())
            {
                for (const auto & task : batch.tasks)
                {
                    if (task.info.bytesize > 0)
                    {
                        result += (task.info.bytesize + chunk_bytesize - 1) / chunk_bytesize;
                    }
                }
            }
        }
        return result;
    }

    std::shared_ptr<Config> config;
    std::shared_ptr<common::Responder> responder;
    std::unique_ptr<Submission> submission;

 private:
    utils::Dylib _dylib;
};

// All requests of a multi-file submission complete successfully through a pool of ObjectStorageWorkers,
// with the drained-responder sentinel randomly enabled to prove the worker tolerates it.
TEST_F(ObjectStorageWorkerTest, Happy_Path)
{
    set_sentinel(utils::random::boolean());

    auto workloads = build(utils::random::number(1, 10), utils::random::number(1, 6));

    {
        auto pool = make_pool(config->s3_concurrency);
        push_all(pool, workloads);

        for (unsigned file_idx = 0; file_idx < submission->paths.size(); ++file_idx)
        {
            for (unsigned i = 0; i < submission->num_chunks[file_idx]; ++i)
            {
                const auto r = responder->pop();
                EXPECT_EQ(r.ret, common::ResponseCode::Success);
                EXPECT_EQ(submission->expected[r.file_index].count(r.index), 1);
                submission->expected[r.file_index].erase(r.index);
            }
        }
    }

    for (const auto & e : submission->expected)
    {
        EXPECT_TRUE(e.empty());
    }
}

// Tearing the pool down while reads are in flight: every request still gets exactly one response (Success
// or FinishedError) - none is lost or double-counted - and the pool joins without hanging.
TEST_F(ObjectStorageWorkerTest, Stopped_Mid_Stream)
{
    set_response_time(1000);   // slow completions -> reads stay in flight

    auto workloads = build(utils::random::number(1, 10), 1);

    {
        auto pool = make_pool(config->s3_concurrency);
        push_all(pool, workloads);

        ::usleep(utils::random::number(100));

        // unblock the workers parked in async_response, then let ~pool set stopped and join
        common::s3::S3ClientWrapper::stop();

        for (unsigned processed = 0; processed < submission->total_requests(); ++processed)
        {
            const auto r = responder->pop();
            EXPECT_TRUE(r.ret == common::ResponseCode::Success || r.ret == common::ResponseCode::FinishedError);
            EXPECT_LT(r.file_index, submission->paths.size());
            EXPECT_LT(r.index, submission->num_chunks[r.file_index]);
            EXPECT_EQ(submission->expected[r.file_index].count(r.index), 1);
            submission->expected[r.file_index].erase(r.index);
        }
    }

    for (const auto & e : submission->expected)
    {
        EXPECT_TRUE(e.empty());
    }
}

// The worker never exceeds the plugin's in-flight window: with one worker (one client) and a bounded
// window, the mock's peak per-client in-flight stays within the window's chunk count.
TEST_F(ObjectStorageWorkerTest, Window_Bounded)
{
    auto workloads = build(utils::random::number(3, 10), 1);

    const size_t window_bytes = config->s3_block_bytesize * utils::random::number<size_t>(2, 6);
    set_window(window_bytes);
    const size_t window_chunks = std::max<size_t>(1, window_bytes / config->s3_block_bytesize);

    {
        auto pool = make_pool(config->s3_concurrency);
        push_all(pool, workloads);

        for (unsigned processed = 0; processed < submission->total_requests(); ++processed)
        {
            EXPECT_EQ(responder->pop().ret, common::ResponseCode::Success);
        }
    }

    const auto peak = max_concurrent();
    EXPECT_LE(peak, window_chunks);
    EXPECT_GT(peak, 0u);
}

// A terminal backend error marked retryable requeues only that ObjectChunk. Every already-completed chunk
// stays complete: total backend submissions grow by exactly one, and all public responses remain Success.
TEST_F(ObjectStorageWorkerTest, RetryableChunkIsRequeuedWithoutRestartingWorkload)
{
    auto workloads = build(1, 1);
    const size_t initial_chunks = count_object_chunks(workloads, config->s3_block_bytesize);
    config->object_storage_retry_timeout = std::chrono::seconds(5);
    set_read_failures(1, common::ResponseCode::RetryableFileAccessError);

    {
        auto pool = make_pool(1);
        push_all(pool, workloads);
        for (unsigned i = 0; i < submission->total_requests(); ++i)
        {
            EXPECT_EQ(responder->pop().ret, common::ResponseCode::Success);
        }
    }

    EXPECT_EQ(total_read_requests(), initial_chunks + 1);
}

// A permanent storage error bypasses the application retry loop even when a retry deadline is configured.
TEST_F(ObjectStorageWorkerTest, PermanentChunkErrorFailsWithoutRetry)
{
    auto workloads = build(1, 1);
    const size_t initial_chunks = count_object_chunks(workloads, config->s3_block_bytesize);
    config->object_storage_retry_timeout = std::chrono::seconds(5);
    set_read_failures(1, common::ResponseCode::FileAccessError);

    bool saw_file_access_error = false;
    {
        auto pool = make_pool(1);
        push_all(pool, workloads);
        for (unsigned i = 0; i < submission->total_requests(); ++i)
        {
            const auto ret = responder->pop().ret;
            saw_file_access_error = saw_file_access_error || ret == common::ResponseCode::FileAccessError;
            EXPECT_TRUE(ret == common::ResponseCode::Success || ret == common::ResponseCode::FileAccessError);
        }
    }

    EXPECT_TRUE(saw_file_access_error);
    EXPECT_EQ(total_read_requests(), initial_chunks);
}

// Once a chunk's retry deadline (started at its first backend submission) has expired, the internal
// retryable marker is converted to the public FileAccessError and no new backend attempt is submitted.
TEST_F(ObjectStorageWorkerTest, ExpiredChunkRetryDeadlineReturnsFileAccessError)
{
    auto workloads = build(1, 1, 1);
    config->s3_block_bytesize = submission->total_bytes + 1;   // exactly one backend chunk
    config->object_storage_retry_timeout = std::chrono::seconds(1);
    const size_t initial_chunks = count_object_chunks(workloads, config->s3_block_bytesize);
    ASSERT_EQ(initial_chunks, 1u);
    set_response_time(1100);   // the first attempt completes after its per-chunk deadline
    set_read_failures(1, common::ResponseCode::RetryableFileAccessError);

    bool saw_file_access_error = false;
    {
        auto pool = make_pool(1);
        push_all(pool, workloads);
        for (unsigned i = 0; i < submission->total_requests(); ++i)
        {
            const auto ret = responder->pop().ret;
            saw_file_access_error = saw_file_access_error || ret == common::ResponseCode::FileAccessError;
            EXPECT_NE(ret, common::ResponseCode::RetryableFileAccessError);
        }
    }

    EXPECT_TRUE(saw_file_access_error);
    EXPECT_EQ(total_read_requests(), initial_chunks);
}

// Time spent before ObjectStorageWorker first submits a chunk does not consume its retry budget. Even after
// waiting longer than the configured timeout, the first retryable completion is requeued and succeeds.
TEST_F(ObjectStorageWorkerTest, RetryDeadlineStartsAtFirstChunkSubmission)
{
    auto workloads = build(1, 1, 1);
    config->s3_block_bytesize = submission->total_bytes + 1;   // exactly one backend chunk
    config->object_storage_retry_timeout = std::chrono::seconds(1);
    const size_t initial_chunks = count_object_chunks(workloads, config->s3_block_bytesize);
    ASSERT_EQ(initial_chunks, 1u);
    set_read_failures(1, common::ResponseCode::RetryableFileAccessError);

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    {
        auto pool = make_pool(1);
        push_all(pool, workloads);
        for (unsigned i = 0; i < submission->total_requests(); ++i)
        {
            EXPECT_EQ(responder->pop().ret, common::ResponseCode::Success);
        }
    }

    EXPECT_EQ(total_read_requests(), initial_chunks + 1);
}

// New submissions pushed WHILE the consumer is draining earlier ones (from a separate thread) all complete:
// the interleaving worker picks up newly arrived workloads without waiting for in-flight ones to finish.
// Every response is checked against its expected (submission, file, request) - not just the total count.
TEST_F(ObjectStorageWorkerTest, Concurrent_Submissions)
{
    make_context(utils::random::number(2, 6));

    // build several submissions up front (each grows the shared responder's expected count). Distinct
    // submission ids (the loop index) let us verify every response back to its exact request.
    const unsigned num_submissions = utils::random::number(2, 5);
    std::vector<std::unique_ptr<Submission>> submissions;
    std::vector<std::vector<Workload>> workloads(num_submissions);
    std::map<std::pair<SubmissionId, unsigned>, std::set<int>> outstanding;   // (submission_id, file_index) -> indices
    unsigned total = 0;
    for (unsigned s = 0; s < num_submissions; ++s)
    {
        submissions.push_back(std::make_unique<Submission>(/*submission_id=*/s, utils::random::number(1, 6), config, responder));
        workloads[s] = submissions[s]->build();
        total += submissions[s]->total_requests();
        for (unsigned f = 0; f < submissions[s]->expected.size(); ++f)
        {
            outstanding[{s, f}] = submissions[s]->expected[f];
        }
    }

    {
        auto pool = make_pool(config->s3_concurrency);
        push_all(pool, workloads[0]);   // first submission on this thread

        // the rest are dispatched concurrently, while the loop below is popping responses
        auto pusher = utils::Thread([&]()
        {
            for (unsigned s = 1; s < num_submissions; ++s)
            {
                ::usleep(utils::random::number(200));
                push_all(pool, workloads[s]);
            }
        });

        for (unsigned processed = 0; processed < total; ++processed)
        {
            const auto r = responder->pop();
            EXPECT_EQ(r.ret, common::ResponseCode::Success);

            auto it = outstanding.find({ r.submission_id, r.file_index });
            ASSERT_NE(it, outstanding.end()) << "response for unknown submission " << r.submission_id << " file " << r.file_index;
            EXPECT_EQ(it->second.erase(r.index), 1u) << "unexpected/duplicate request index " << r.index;
        }

        pusher.join();
    }

    // every expected response arrived exactly once, so nothing is left outstanding and the responder drained
    for (const auto & [key, indices] : outstanding)
    {
        EXPECT_TRUE(indices.empty()) << "submission " << key.first << " file " << key.second << " missing responses";
    }
    EXPECT_EQ(responder->pop().ret, common::ResponseCode::FinishedError);
}

// An empty workload (no batches) is a no-op: the worker logs a warning, builds no client, issues no reads,
// and stays idle so the pool joins cleanly. Production never dispatches empty workloads (they are skipped
// at dispatch), but the worker must not crash or hang on one.
TEST_F(ObjectStorageWorkerTest, Empty_Workload)
{
    {
        auto pool = make_pool(utils::random::number(1, 4));
        pool.push(Workload{});
        pool.push(Workload{});
    }

    // no client was ever created and nothing was read
    EXPECT_EQ(clients(), 0);
}

}; // namespace runai::llm::streamer::impl
