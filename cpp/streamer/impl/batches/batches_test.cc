#include "streamer/impl/batches/batches.h"

#include <gtest/gtest.h>
#include <memory>
#include <set>

#include "utils/random/random.h"
#include "utils/temp/file/file.h"

#include "common/exception/exception.h"

namespace runai::llm::streamer::impl
{

TEST(Batches, Sanity)
{
    auto size = utils::random::number(1000, 100000);
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    // create internal division to divide the file into requests (each request represent a tensor)
    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    auto config = std::make_shared<Config>(utils::random::number(1, 20), utils::random::number<size_t>(1, 1024), chunk_size, false /* do not force minimum chunk size */);

    std::vector<char> dst(size);
    auto responder = std::make_shared<common::Responder>(num_chunks);

    size_t total_bytes = 0;
    std::vector<unsigned> covered(size);

    {
        Batches batches(config, responder, file.path, nullptr, 0, size, dst.data(), num_chunks, chunks.data());

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

            batch.finished_until(batch.range.end);
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
        Batches batches(config, responder, utils::random::string(), nullptr, 0, size, dst.data(), num_chunks, chunks.data());
    }
    catch(const common::Exception & e)
    {
        ret = e.error();
    }

    // reader is created later when the batch is executed, and that is when the error will occur
    EXPECT_EQ(ret, common::ResponseCode::Success);
    responder->cancel();
}

}; // namespace runai::llm::streamer::impl
