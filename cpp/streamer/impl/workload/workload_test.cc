#include "streamer/impl/workload/workload.h"

#include <gtest/gtest.h>
#include <memory>
#include <atomic>
#include <numeric>
#include <set>
#include <string>
#include <utility>

#include "streamer/impl/assigner/assigner.h"

#include "utils/random/random.h"
#include "utils/temp/file/file.h"
#include "utils/thread/thread.h"
#include "utils/scope_guard/scope_guard.h"
#include "utils/semaphore/semaphore.h"
#include "common/exception/exception.h"
#include "utils/logging/logging.h"
#include "utils/dylib/dylib.h"

namespace runai::llm::streamer::impl
{

TEST(Workload, Sanity)
{
    auto num_files = utils::random::number(1, 10);
    LOG(DEBUG) << "number of files " << num_files;

    common::s3::S3ClientWrapper::Params s3_params;

    std::atomic<bool> stopped(false);
    std::vector<std::string> paths;
    std::vector<size_t> file_offsets;
    std::vector<size_t> bytesizes;
    std::vector<void*> dsts;
    std::vector<std::vector<uint8_t>> data(num_files);
    std::vector<unsigned> num_chunks(num_files);
    std::vector<utils::temp::File> file;
    std::vector<std::set<int>> expected_responses(num_files);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    auto config = std::make_shared<Config>(utils::random::number(1, 20), utils::random::number(1, 20), utils::random::number<size_t>(1, 1024), chunk_size, false /* do not force minimum chunk size */);
    auto responder = std::make_shared<common::Responder>(0);

    size_t total_bytes = 0;
    for (unsigned i = 0; i < num_files; ++i)
    {
        auto size = utils::random::number(1000, 100000);
        num_chunks[i] = utils::random::number(1, 20);
        EXPECT_LT(num_chunks[i], size);
        responder->increment(num_chunks[i]);
        total_bytes += size;
        data[i] = utils::random::buffer(size);
        file.push_back(utils::temp::File(data[i]));

        paths.push_back(file[i].path);
        file_offsets.push_back(0);
        bytesizes.push_back(size);
    }

    std::vector<char> buffer(total_bytes);
    dsts.push_back(buffer.data());

    {
        Assigner assigner(paths, file_offsets, bytesizes, dsts, config);

        EXPECT_GT(assigner.file_assignments(0).size(), 0);
        EXPECT_LE(assigner.file_assignments(0).size(), config->concurrency);
        EXPECT_LE(assigner.num_workloads(), config->concurrency);

        std::vector<Workload> workloads(assigner.num_workloads());

        for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
        {
            auto chunks = utils::random::chunks(bytesizes[file_idx], num_chunks[file_idx]);
            EXPECT_EQ(chunks.size(), num_chunks[file_idx]);
            auto total_chunks_size = std::accumulate(chunks.begin(), chunks.end(), 0u);
            EXPECT_EQ(total_chunks_size, bytesizes[file_idx]);

            Batches batches(file_idx, assigner.file_assignments(file_idx), config, responder, file[file_idx].path, s3_params, chunks);

            for (size_t j = 0; j < batches.size(); ++j)
            {
                auto & batch = batches[j];
                workloads[batch.worker_index].add_batch(std::move(batch));
            }

            for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
            {
                expected_responses[file_idx].insert(i);
            }
        }
        // execute workloads
        for (auto & workload : workloads)
        {
            workload.execute(stopped);
        }
    }

    // wait for all the requests to finish

    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
        {
            const auto r = responder->pop();
            EXPECT_EQ(r.ret, common::ResponseCode::Success);
            EXPECT_EQ(expected_responses[file_idx].count(r.index), 1);
            expected_responses[file_idx].erase(r.index);
        }
        EXPECT_TRUE(expected_responses[file_idx].empty());
    }

    auto r = responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);

    // verify read
    size_t offset = 0;

    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        const auto & expected_content = data[file_idx];
        for (unsigned i = 0; i < expected_content.size(); ++i)
        {
            EXPECT_EQ(static_cast<char>(expected_content[i]), buffer[offset + i]);
        }
        offset += expected_content.size();
    }
}

TEST(Workload, Stopped)
{
    auto num_files = utils::random::number(1, 10);

    common::s3::S3ClientWrapper::Params s3_params;

    std::atomic<bool> stopped(false);
    std::vector<std::string> paths;
    std::vector<size_t> file_offsets;
    std::vector<size_t> bytesizes;
    std::vector<void*> dsts;
    std::vector<std::vector<uint8_t>> data(num_files);
    std::vector<unsigned> num_chunks(num_files);
    std::vector<utils::temp::File> file;
    std::vector<std::set<int>> expected_responses(num_files);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    auto config = std::make_shared<Config>(1, 1, utils::random::number<size_t>(1, 1024), chunk_size, false /* do not force minimum chunk size */);
    auto responder = std::make_shared<common::Responder>(0);

    size_t total_bytes = 0;
    for (unsigned i = 0; i < num_files; ++i)
    {
        auto size = utils::random::number(1000, 100000);
        num_chunks[i] = utils::random::number(1, 20);
        EXPECT_LT(num_chunks[i], size);
        responder->increment(num_chunks[i]);
        total_bytes += size;
        data[i] = utils::random::buffer(size);
        file.push_back(utils::temp::File(data[i]));

        paths.push_back(file[i].path);
        file_offsets.push_back(0);
        bytesizes.push_back(size);
    }

    std::vector<char> buffer(total_bytes);
    dsts.push_back(buffer.data());

    Assigner assigner(paths, file_offsets, bytesizes, dsts, config);

    EXPECT_GT(assigner.file_assignments(0).size(), 0);
    EXPECT_LE(assigner.file_assignments(0).size(), config->concurrency);


    Workload workload;

    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        auto chunks = utils::random::chunks(bytesizes[file_idx], num_chunks[file_idx]);
        EXPECT_EQ(chunks.size(), num_chunks[file_idx]);
        auto total_chunks_size = std::accumulate(chunks.begin(), chunks.end(), 0u);
        EXPECT_EQ(total_chunks_size, bytesizes[file_idx]);

        Batches batches(file_idx, assigner.file_assignments(file_idx), config, responder, file[file_idx].path, s3_params, chunks);

        for (size_t j = 0; j < batches.size(); ++j)
        {
            auto & batch = batches[j];
            workload.add_batch(std::move(batch));
        }

        for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
        {
            expected_responses[file_idx].insert(i);
        }
    }

    // execute workloads
    auto thread = utils::Thread([&]()
    {
        workload.execute(stopped);
    });

    ::usleep(utils::random::number(300));

    common::s3::S3ClientWrapper::stop();
    stopped = true;

    // wait for all the requests to finish

    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
        {
            const auto r = responder->pop();
            EXPECT_TRUE(r.ret == common::ResponseCode::Success || r.ret == common::ResponseCode::FinishedError);
            EXPECT_LT(r.file_index, num_files);
            EXPECT_LT(r.index, num_chunks[r.file_index]);
            EXPECT_EQ(expected_responses[r.file_index].count(r.index), 1);
            expected_responses[r.file_index].erase(r.index);
        }
    }
    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        EXPECT_TRUE(expected_responses[file_idx].empty());
    }

    auto r = responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
}

TEST(Workload, Stopped_Async)
{
    // mock S3
    utils::Dylib dylib("libstreamers3.so");
    auto mock_response_time = dylib.dlsym<void(*)(unsigned)>("runai_mock_s3_set_response_time_ms");
    auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");
    unsigned delay_ms = 1000;
    mock_response_time(delay_ms);
    auto guard = utils::ScopeGuard([&mock_cleanup](){
        mock_cleanup();
    });

    auto num_files = utils::random::number(1, 10);

    common::s3::Credentials credentials(
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr));

    std::atomic<bool> stopped(false);
    std::vector<std::string> paths;
    std::vector<size_t> file_offsets;
    std::vector<size_t> bytesizes;
    std::vector<void*> dsts;
    std::vector<unsigned> num_chunks(num_files);
    std::vector<std::set<int>> expected_responses(num_files);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    auto config = std::make_shared<Config>(1, 1, utils::random::number<size_t>(1, 1024), chunk_size, false /* do not force minimum chunk size */);
    auto responder = std::make_shared<common::Responder>(0);

    size_t total_bytes = 0;
    std::string bucket = "test-bucket";
    for (unsigned i = 0; i < num_files; ++i)
    {
        auto size = utils::random::number(1000, 100000);
        num_chunks[i] = utils::random::number(1, 20);
        EXPECT_LT(num_chunks[i], size);
        responder->increment(num_chunks[i]);
        total_bytes += size;

        paths.push_back("s3://" + bucket + "/" + utils::random::string());
        file_offsets.push_back(0);
        bytesizes.push_back(size);
    }

    std::vector<char> buffer(total_bytes);
    dsts.push_back(buffer.data());

    Assigner assigner(paths, file_offsets, bytesizes, dsts, config);

    EXPECT_GT(assigner.file_assignments(0).size(), 0);
    EXPECT_LE(assigner.file_assignments(0).size(), config->s3_concurrency);

    Workload workload;
    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        auto chunks = utils::random::chunks(bytesizes[file_idx], num_chunks[file_idx]);
        EXPECT_EQ(chunks.size(), num_chunks[file_idx]);
        auto total_chunks_size = std::accumulate(chunks.begin(), chunks.end(), 0u);
        EXPECT_EQ(total_chunks_size, bytesizes[file_idx]);

        std::shared_ptr<common::s3::StorageUri> uri;
        EXPECT_NO_THROW(uri = std::make_shared<common::s3::StorageUri>(paths[file_idx]));
        common::s3::S3ClientWrapper::Params s3_params(uri, credentials, utils::random::number<size_t>());

        Batches batches(file_idx, assigner.file_assignments(file_idx), config, responder, paths[file_idx], s3_params, chunks);

        for (size_t j = 0; j < batches.size(); ++j)
        {
            auto & batch = batches[j];
            workload.add_batch(std::move(batch));
        }

        for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
        {
            expected_responses[file_idx].insert(i);
        }
    }

    // execute workloads
    auto sem = utils::Semaphore(0);

    auto thread = utils::Thread([&]()
    {
        sem.post();
        workload.execute(stopped);
    });

    sem.wait();
    ::usleep(utils::random::number(100));

    common::s3::S3ClientWrapper::stop();
    stopped = true;

    // wait for all the requests to finish

    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
        {
            const auto r = responder->pop();
            EXPECT_TRUE(r.ret == common::ResponseCode::Success || r.ret == common::ResponseCode::FinishedError);
            EXPECT_LT(r.file_index, num_files);
            EXPECT_LT(r.index, num_chunks[r.file_index]);
            EXPECT_EQ(expected_responses[r.file_index].count(r.index), 1);
            expected_responses[r.file_index].erase(r.index);
        }
    }
    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        EXPECT_TRUE(expected_responses[file_idx].empty());
    }

    auto r = responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
}

}; // namespace runai::llm::streamer::impl
