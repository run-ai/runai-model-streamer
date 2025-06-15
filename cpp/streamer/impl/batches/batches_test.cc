#include "streamer/impl/batches/batches.h"

#include <gtest/gtest.h>
#include <memory>
#include <set>

#include "streamer/impl/assigner/assigner.h"

#include "utils/random/random.h"
#include "utils/temp/file/file.h"

#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

TEST(Batches, Sanity)
{
    auto num_files = utils::random::number(1, 10);
    LOG(DEBUG) << "number of files " << num_files;

    common::s3::S3ClientWrapper::Params s3_params;

    std::vector<std::string> paths;
    std::vector<size_t> file_offsets;
    std::vector<size_t> bytesizes;
    std::vector<void*> dsts;
    std::vector<std::vector<unsigned>> covered(num_files);
    std::vector<size_t> total_bytes(num_files);
    std::vector<std::vector<uint8_t>> data(num_files);
    std::vector<std::vector<char>> buffers(num_files);
    std::vector<unsigned> num_chunks(num_files);
    std::vector<utils::temp::File> file;
    std::vector<std::set<int>> expected_responses(num_files);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    auto config = std::make_shared<Config>(utils::random::number(1, 20), utils::random::number<size_t>(1, 1024), chunk_size, false /* do not force minimum chunk size */);
    auto responder = std::make_shared<common::Responder>(0);

    for (unsigned i = 0; i < num_files; ++i)
    {
        auto size = utils::random::number(1000, 100000);
        num_chunks[i] = utils::random::number(1, 20);
        EXPECT_LT(num_chunks[i], size);
        responder->increment(num_chunks[i]);

        data[i] = utils::random::buffer(size);
        file.push_back(utils::temp::File(data[i]));

        std::vector<char> dst(size);
        buffers[i] = dst;

        total_bytes[i] = 0;
        covered[i].resize(size);

        paths.push_back(file[i].path);
        file_offsets.push_back(0);
        bytesizes.push_back(size);
        dsts.push_back(buffers[i].data());
    }
    {
        Assigner assigner(paths, file_offsets, bytesizes, dsts, config);

        EXPECT_GT(assigner.file_assignments(0).size(), 0);
        EXPECT_LE(assigner.file_assignments(0).size(), config->concurrency);

        // create internal division to divide the file into requests (each request represent a tensor)

        for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
        {
            auto chunks = utils::random::chunks(bytesizes[file_idx], num_chunks[file_idx]);
            EXPECT_EQ(chunks.size(), num_chunks[file_idx]);
            auto total_chunks_size = std::accumulate(chunks.begin(), chunks.end(), 0u);
            EXPECT_EQ(total_chunks_size, bytesizes[file_idx]);

            Batches batches(utils::random::number(), assigner.file_assignments(file_idx), config, responder, file[file_idx].path, s3_params, chunks);

            // execute tasks
            for (unsigned i = 0; i < batches.size(); ++i)
            {
                auto & batch = batches[i];
                for (auto & task : batch.tasks)
                {
                    total_bytes[file_idx] += task.info.bytesize;
                    for (unsigned j = task.info.offset; j < task.info.end; ++j)
                    {
                        covered[file_idx][j] += 1;
                    }
                }

                batch.finished_until(batch.end_offset());
            }

            for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
            {
                expected_responses[file_idx].insert(i);
            }
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

    // verify that the entire range is covered
    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        EXPECT_EQ(total_bytes[file_idx], bytesizes[file_idx]);
        for (auto byte : covered[file_idx])
        {
            EXPECT_EQ(byte, 1);
        }
    }
}

TEST(Batches, Failed_Reader)
{
    // create empty file
    auto size = utils::random::number(100, 10000);

    // create internal division
    const unsigned num_chunks = utils::random::number(2, 20);
    EXPECT_LT(num_chunks, size);

    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    auto config = std::make_shared<Config>(utils::random::number(1, 20), utils::random::number<size_t>(1, 1024), chunk_size, false /* do not force minimum chunk size */);

    std::vector<char> dst(size);
    auto responder = std::make_shared<common::Responder>(num_chunks);

    common::ResponseCode ret = common::ResponseCode::Success;

    try
    {
        common::s3::S3ClientWrapper::Params s3_params;

        std::vector<std::string> paths;
        std::vector<size_t> file_offsets;
        std::vector<size_t> bytesizes;
        std::vector<void*> dsts;

        const auto file_path = utils::random::string();
        paths.push_back(file_path);
        file_offsets.push_back(0);
        bytesizes.push_back(size);
        dsts.push_back(dst.data());

        Assigner assigner(paths, file_offsets, bytesizes, dsts, config);

        EXPECT_GT(assigner.file_assignments(0).size(), 0);
        EXPECT_LE(assigner.file_assignments(0).size(), config->concurrency);

        Batches batches(utils::random::number(), assigner.file_assignments(0), config, responder, file_path, s3_params, chunks);
    }
    catch(const common::Exception & e)
    {
        ret = e.error();
    }

    // reader is created later when the batch is executed, and that is when the error will occur
    EXPECT_EQ(ret, common::ResponseCode::Success);
    responder->cancel();
}

TEST(Batches, Zero_Size_Request)
{
    auto size = utils::random::number(1000, 100000);
    unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    // create internal division to divide the file into requests (each request represent a tensor)
    auto non_zero_chunks = utils::random::chunks(size, num_chunks);

    // add zero size chunks
    std::vector<size_t> chunks;

    unsigned num_zero_chunks = utils::random::number(0, 2);


    for (unsigned i = 0; i < num_chunks;)
    {
        bool add_zero = utils::random::boolean();
        if (num_zero_chunks > 0 && add_zero)
        {
            chunks.push_back(0);
            --num_zero_chunks;
        }
        else
        {
            chunks.push_back(non_zero_chunks[i]);
            ++i;
        }
    }

    num_chunks = chunks.size();

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    auto config = std::make_shared<Config>(utils::random::number(1, 20), utils::random::number<size_t>(1, 1024), chunk_size, false /* do not force minimum chunk size */);

    std::vector<char> dst(size);
    auto responder = std::make_shared<common::Responder>(num_chunks);

    size_t total_bytes = 0;
    std::vector<unsigned> covered(size);

    {
        common::s3::S3ClientWrapper::Params s3_params;

        std::vector<std::string> paths;
        std::vector<size_t> file_offsets;
        std::vector<size_t> bytesizes;
        std::vector<void*> dsts;

        paths.push_back(file.path);
        file_offsets.push_back(0);
        bytesizes.push_back(size);
        dsts.push_back(dst.data());

        Assigner assigner(paths, file_offsets, bytesizes, dsts, config);

        EXPECT_GT(assigner.file_assignments(0).size(), 0);
        EXPECT_LE(assigner.file_assignments(0).size(), config->concurrency);

        Batches batches(utils::random::number(), assigner.file_assignments(0), config, responder, file.path, s3_params, chunks);

        // execute tasks
        for (unsigned i = 0; i < batches.size(); ++i)
        {
            auto & batch = batches[i];
            for (auto & task : batch.tasks)
            {
                total_bytes += task.info.bytesize;
                for (unsigned j = task.info.offset; j < task.info.end; ++j)
                {
                    covered[j] += 1;
                }
            }

            batch.finished_until(batch.end_offset());
        }
    }

    // wait for all the requests to finish
    std::set<int> expected_responses;

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        expected_responses.insert(i);
    }

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        const auto r = responder->pop();
        EXPECT_EQ(r.ret, common::ResponseCode::Success);
        EXPECT_EQ(expected_responses.count(r.index), 1);
        expected_responses.erase(r.index);
    }

    EXPECT_TRUE(expected_responses.empty());
    auto r = responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);

    // verify that the entire range is covered
    EXPECT_EQ(total_bytes, size);
    for (auto byte : covered)
    {
        EXPECT_EQ(byte, 1);
    }
}

}; // namespace runai::llm::streamer::impl
