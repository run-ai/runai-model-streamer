#include "streamer/impl/object_storage_worker/object_storage_worker.h"

#include <gtest/gtest.h>

#include <atomic>
#include <map>
#include <memory>
#include <numeric>
#include <set>
#include <string>
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
    Submission(unsigned submission_id, unsigned num_files, std::shared_ptr<Config> config, std::shared_ptr<common::Responder> responder) :
        submission_id(submission_id),
        config(config),
        responder(responder),
        num_chunks(num_files),
        expected(num_files)
    {
        const std::string bucket = "test-bucket";
        for (unsigned i = 0; i < num_files; ++i)
        {
            const auto size = utils::random::number(1000, 100000);
            num_chunks[i] = utils::random::number(1, 20);
            EXPECT_LT(num_chunks[i], size);
            responder->increment(num_chunks[i]);
            total_bytes += size;

            paths.push_back("s3://" + bucket + "/" + utils::random::string());
            file_offsets.push_back(0);
            bytesizes.push_back(size);
        }
        buffer.resize(total_bytes);
        dsts.push_back(buffer.data());
    }

    // build the workloads (one Assigner over all files, Batches per file) ready to push to the pool
    std::vector<Workload> build()
    {
        Assigner assigner(paths, file_offsets, bytesizes, dsts, config);
        std::vector<Workload> workloads(assigner.num_workloads());

        const common::s3::Credentials credentials;
        for (unsigned file_idx = 0; file_idx < paths.size(); ++file_idx)
        {
            auto chunks = utils::random::chunks(bytesizes[file_idx], num_chunks[file_idx]);
            EXPECT_EQ(chunks.size(), num_chunks[file_idx]);

            auto uri = std::make_shared<common::s3::StorageUri>(paths[file_idx]);
            common::s3::S3ClientWrapper::Params params(uri, credentials, config->s3_block_bytesize);

            Batches batches(submission_id, file_idx, assigner.file_assignments(file_idx), config, responder, paths[file_idx], params, chunks);
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

    unsigned submission_id;
    std::shared_ptr<Config> config;
    std::shared_ptr<common::Responder> responder;
    std::vector<std::string> paths;
    std::vector<size_t> file_offsets;
    std::vector<size_t> bytesizes;
    std::vector<void*> dsts;
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
    std::vector<Workload> build(unsigned num_files, unsigned s3_concurrency)
    {
        make_context(s3_concurrency);
        submission = std::make_unique<Submission>(utils::random::number(), num_files, config, responder);
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
    size_t max_concurrent()             { return _dylib.dlsym<size_t(*)()>("runai_mock_s3_max_concurrent")(); }
    int clients()                       { return _dylib.dlsym<int(*)()>("runai_mock_s3_clients")(); }

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
    std::map<std::pair<unsigned, unsigned>, std::set<int>> outstanding;   // (submission_id, file_index) -> indices
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
