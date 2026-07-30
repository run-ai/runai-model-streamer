#include "streamer/impl/batches/batches.h"

#include <gtest/gtest.h>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "streamer/impl/assigner/assigner.h"

#include "utils/random/random.h"
#include "utils/temp/file/file.h"

#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

namespace
{

// A file whose ranges tile [0, sum(sizes)) and are packed consecutively from dst. Coalesces to exactly
// one ContiguousTransfer, which is the layout the previous single-destination API implied.
FileRanges contiguous_file(const std::string & path, const std::vector<size_t> & sizes, char * dst)
{
    FileRanges file;
    file.path = path;
    file.ranges.reserve(sizes.size());

    size_t offset = 0;
    char * d = dst;
    for (const auto size : sizes)
    {
        file.ranges.push_back(ReadRange{ offset, size, d });
        offset += size;
        d += size;
    }
    return file;
}

} // namespace

TEST(Batches, Sanity)
{
    auto num_files = utils::random::number(1, 10);
    LOG(DEBUG) << "number of files " << num_files;

    common::s3::S3ClientWrapper::Params s3_params;

    std::vector<FileRanges> request;
    std::vector<std::vector<unsigned>> covered(num_files);
    std::vector<size_t> total_bytes(num_files);
    std::vector<std::vector<uint8_t>> data(num_files);
    std::vector<std::vector<char>> buffers(num_files);
    std::vector<std::vector<size_t>> chunks(num_files);
    std::vector<unsigned> num_chunks(num_files);
    std::vector<size_t> bytesizes(num_files);
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

        buffers[i].resize(size);
        total_bytes[i] = 0;
        covered[i].resize(size);
        bytesizes[i] = size;

        // the ranges must exist before the Assigner is built - it coalesces them
        chunks[i] = utils::random::chunks(size, num_chunks[i]);
        EXPECT_EQ(chunks[i].size(), num_chunks[i]);
        EXPECT_EQ(std::accumulate(chunks[i].begin(), chunks[i].end(), static_cast<size_t>(0)), size);

        request.push_back(contiguous_file(file[i].path, chunks[i], buffers[i].data()));

        for (unsigned j = 0; j < num_chunks[i]; ++j)
        {
            expected_responses[i].insert(j);
        }
    }
    {
        Assigner assigner(request, config);

        // each file's ranges are adjacent in both file and buffer, so each file is exactly one transfer
        ASSERT_EQ(assigner.transfers().size(), num_files);

        for (const auto & transfer : assigner.transfers())
        {
            const auto file_idx = transfer.file_index;

            EXPECT_GT(transfer.tasks.size(), 0);
            EXPECT_LE(transfer.tasks.size(), config->concurrency);

            Batches batches(utils::random::number(), file_idx, transfer.tasks, config, responder,
                            request[file_idx].path, s3_params, transfer.range_sizes, transfer.first_range_index);

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
        }
    }

    // wait for all the requests to finish

    for (unsigned file_idx = 0; file_idx < num_files; ++file_idx)
    {
        for (unsigned i = 0; i < num_chunks[file_idx]; ++i)
        {
            const auto r = responder->pop();
            EXPECT_EQ(r.ret, common::ResponseCode::Success);
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

        const auto file_path = utils::random::string();

        std::vector<FileRanges> request;
        request.push_back(contiguous_file(file_path, chunks, dst.data()));

        Assigner assigner(request, config);

        ASSERT_EQ(assigner.transfers().size(), 1);
        const auto & transfer = assigner.transfers()[0];
        EXPECT_GT(transfer.tasks.size(), 0);
        EXPECT_LE(transfer.tasks.size(), config->concurrency);

        Batches batches(utils::random::number(), transfer.file_index, transfer.tasks, config, responder,
                        file_path, s3_params, transfer.range_sizes, transfer.first_range_index);
    }
    catch(const common::Exception & e)
    {
        ret = e.error();
    }

    // reader is created later when the batch is executed, and that is when the error will occur
    EXPECT_EQ(ret, common::ResponseCode::Success);
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

        std::vector<FileRanges> request;
        request.push_back(contiguous_file(file.path, chunks, dst.data()));

        Assigner assigner(request, config);

        // a zero sized range neither advances the file offset nor the destination, so it stays inside the
        // same transfer as its neighbours - the whole file is still one contiguous read
        ASSERT_EQ(assigner.transfers().size(), 1);
        const auto & transfer = assigner.transfers()[0];
        EXPECT_EQ(transfer.range_sizes.size(), num_chunks);
        EXPECT_GT(transfer.tasks.size(), 0);
        EXPECT_LE(transfer.tasks.size(), config->concurrency);

        Batches batches(utils::random::number(), transfer.file_index, transfer.tasks, config, responder,
                        file.path, s3_params, transfer.range_sizes, transfer.first_range_index);

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
