#include "streamer/impl/batches/batches.h"

#include <gtest/gtest.h>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "streamer/impl/assigner/assigner.h"

#include "utils/random/random.h"
#include "utils/temp/env/env.h"
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

// Tasks must be cut at chunk boundaries as well as at worker boundaries, so a completed chunk always
// covers a whole number of tasks.
//
// Nothing above asserts this: the other tests use the default 8 MiB chunk against fixtures of a few
// hundred KB, so no cut ever happens and they pass whether or not the code cuts at all.
//
// The chunk size arrives through the environment, so this also covers the plumbing from
// RUNAI_STREAMER_FS_CHUNK_BYTESIZE to the field. Passing it to the constructor instead would pass
// even if nothing ever read the variable.
TEST(Batches, Tasks_Are_Cut_At_Chunk_Boundaries)
{
    constexpr size_t chunk = 4096;

    // One worker, so the only cuts are the chunk ones and the expected shape is exact.
    utils::temp::Env concurrency(std::string("RUNAI_STREAMER_CONCURRENCY"), 1UL);
    utils::temp::Env chunk_bytesize(std::string("RUNAI_STREAMER_FS_CHUNK_BYTESIZE"), static_cast<unsigned long>(chunk));

    auto config = std::make_shared<Config>(false /* do not force minimum */);
    ASSERT_EQ(config->fs_async_chunk_bytesize, chunk);
    ASSERT_EQ(config->concurrency, 1);

    auto responder = std::make_shared<common::Responder>(0);
    common::s3::S3ClientWrapper::Params s3_params;

    // The second range does NOT start on a chunk boundary, which is the case that matters: boundaries
    // are absolute file offsets, so its first task is short and the rest are whole chunks.
    const std::vector<size_t> sizes = { 1000, 20000 };
    const size_t total = 21000;

    auto data = utils::random::buffer(total);
    utils::temp::File file(data);
    std::vector<char> buffer(total);

    std::vector<FileRanges> request = { contiguous_file(file.path, sizes, buffer.data()) };

    Assigner assigner(request, config);
    ASSERT_EQ(assigner.transfers().size(), 1);

    const auto & transfer = assigner.transfers().front();
    Batches batches(utils::random::number(), transfer.file_index, transfer.tasks, config, responder,
                    request[0].path, s3_params, transfer.range_sizes, transfer.first_range_index);

    ASSERT_EQ(batches.size(), 1);

    std::vector<std::pair<size_t, size_t>> seen;   // offset, bytesize - in task order
    for (const auto & task : batches[0].tasks)
    {
        seen.emplace_back(task.info.offset, task.info.bytesize);
    }

    // No task straddles a chunk boundary: its first and last byte fall in the same chunk.
    for (const auto & entry : seen)
    {
        ASSERT_GT(entry.second, 0);
        EXPECT_EQ(entry.first / chunk, (entry.first + entry.second - 1) / chunk)
            << "task [" << entry.first << ", " << entry.first + entry.second << ") crosses a "
            << chunk << " boundary";
    }

    // The exact cut, not only the property - a wrong cut can still satisfy "no straddle".
    const std::vector<std::pair<size_t, size_t>> expected = {
        { 0, 1000 },        // range 0, entirely inside chunk 0
        { 1000, 3096 },     // range 1 starts mid-chunk, so its first task ends at 4096
        { 4096, 4096 },
        { 8192, 4096 },
        { 12288, 4096 },
        { 16384, 4096 },
        { 20480, 520 },     // and its last task is whatever is left
    };
    EXPECT_EQ(seen, expected);

    // Cutting must not lose or duplicate bytes: the tasks still tile the file exactly once.
    size_t covered = 0;
    for (const auto & entry : seen)
    {
        EXPECT_EQ(entry.first, covered);
        covered += entry.second;
    }
    EXPECT_EQ(covered, total);
}

// A range that fits inside one chunk is one task, so the cut costs nothing when it is not needed.
TEST(Batches, Small_Ranges_Are_Not_Cut)
{
    utils::temp::Env concurrency(std::string("RUNAI_STREAMER_CONCURRENCY"), 1UL);
    utils::temp::Env chunk_bytesize(std::string("RUNAI_STREAMER_FS_CHUNK_BYTESIZE"), 1UL << 20);

    auto config = std::make_shared<Config>(false);
    auto responder = std::make_shared<common::Responder>(0);
    common::s3::S3ClientWrapper::Params s3_params;

    const std::vector<size_t> sizes = { 100, 200, 300 };
    auto data = utils::random::buffer(600);
    utils::temp::File file(data);
    std::vector<char> buffer(600);

    std::vector<FileRanges> request = { contiguous_file(file.path, sizes, buffer.data()) };

    Assigner assigner(request, config);
    const auto & transfer = assigner.transfers().front();
    Batches batches(utils::random::number(), transfer.file_index, transfer.tasks, config, responder,
                    request[0].path, s3_params, transfer.range_sizes, transfer.first_range_index);

    ASSERT_EQ(batches.size(), 1);
    EXPECT_EQ(batches[0].tasks.size(), sizes.size());
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
