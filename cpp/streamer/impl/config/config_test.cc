#include "streamer/impl/config/config.h"

#include <gtest/gtest.h>
#include <algorithm>

#include "common/s3_wrapper/s3_wrapper.h"

#include "utils/random/random.h"
#include "utils/temp/env/env.h"

namespace runai::llm::streamer::impl
{

TEST(Creation, Default)
{
    Config config;
    EXPECT_EQ(config.concurrency, 16UL);
    EXPECT_EQ(config.s3_concurrency, 8UL);
    EXPECT_EQ(config.s3_block_bytesize, 8 * 1024 * 1024);
    EXPECT_EQ(config.fs_block_bytesize, 2 * 1024 * 1024);
}

TEST(Creation, Concurrency)
{
    const auto expected = utils::random::number<int>(1, 1000);
    utils::temp::Env size_("RUNAI_STREAMER_CONCURRENCY", expected);

    Config config;
    EXPECT_EQ(config.concurrency, expected);
    EXPECT_EQ(config.s3_concurrency, expected);
    EXPECT_EQ(config.s3_block_bytesize, 8 * 1024 * 1024);
    EXPECT_EQ(config.fs_block_bytesize, 2 * 1024 * 1024);
}

TEST(Creation, Chunk_Size)
{
    const size_t min_ = common::obj_store::S3ClientWrapper::min_chunk_bytesize;
    for (size_t expected : { 1UL, utils::random::number<size_t>(1UL, min_ - 1UL), utils::random::number<size_t>(min_, 10UL * min_)})
    {
        utils::temp::Env size_("RUNAI_STREAMER_CHUNK_BYTESIZE", expected);
        Config config;

        EXPECT_EQ(config.concurrency, 16UL);
        EXPECT_EQ(config.s3_concurrency, 8UL);
        EXPECT_EQ(config.s3_block_bytesize, std::max(expected, min_));
        EXPECT_EQ(config.fs_block_bytesize, std::max(expected, Config::min_fs_block_bytesize));
    }
}

TEST(Creation, Zero_Chunk_Size)
{
    utils::temp::Env size_("RUNAI_STREAMER_CHUNK_BYTESIZE", 0);
    EXPECT_THROW(Config(), std::exception);
}

TEST(Creation, Zero_Concurrency)
{
    utils::temp::Env size_("RUNAI_STREAMER_CONCURRENCY", 0);
    EXPECT_THROW(Config(), std::exception);
}

}; // namespace runai::llm::streamer::impl
