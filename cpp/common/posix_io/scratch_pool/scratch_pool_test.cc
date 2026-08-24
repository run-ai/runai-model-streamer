#include "common/posix_io/scratch_pool/scratch_pool.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <set>
#include <vector>

namespace runai::llm::streamer::common::posix_io
{

// The one property the kernel checks. A buffer that is not block aligned fails the read with EINVAL.
TEST(ScratchPool, Every_Buffer_Is_Aligned)
{
    constexpr size_t block = 4096;
    ScratchPool pool(8, block);

    for (size_t i = 0; i < 8; ++i)
    {
        char * buffer = pool.take();
        ASSERT_NE(buffer, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(buffer) % block, 0u) << "buffer " << i << " is not aligned";
    }
}

// Two reads in flight must never be handed the same memory.
TEST(ScratchPool, Buffers_Are_Distinct)
{
    ScratchPool pool(16, 4096);

    std::set<char *> seen;
    for (size_t i = 0; i < 16; ++i)
    {
        char * buffer = pool.take();
        ASSERT_NE(buffer, nullptr);
        EXPECT_TRUE(seen.insert(buffer).second) << "the same buffer was handed out twice";
    }
}

// Sized to the in-flight window, so it can be emptied but should not be in practice.
TEST(ScratchPool, Runs_Out_Rather_Than_Overrunning)
{
    ScratchPool pool(2, 4096);

    EXPECT_EQ(pool.free_count(), 2u);
    char * first = pool.take();
    char * second = pool.take();
    EXPECT_EQ(pool.free_count(), 0u);

    // nullptr, not a crash and not a reused buffer: the caller can read buffered instead.
    EXPECT_EQ(pool.take(), nullptr);

    pool.give(first);
    EXPECT_EQ(pool.free_count(), 1u);
    EXPECT_NE(pool.take(), nullptr);

    pool.give(second);
}

// A returned buffer is handed out again, so a long run does not need more than the window.
TEST(ScratchPool, Returned_Buffers_Are_Reused)
{
    ScratchPool pool(1, 4096);

    for (int i = 0; i < 100; ++i)
    {
        char * buffer = pool.take();
        ASSERT_NE(buffer, nullptr) << "exhausted on round " << i;
        pool.give(buffer);
    }

    EXPECT_EQ(pool.free_count(), 1u);
}

// The buffers must be usable, not merely handed out - so write to every byte of one.
TEST(ScratchPool, Buffers_Hold_A_Whole_Block)
{
    constexpr size_t block = 4096;
    ScratchPool pool(4, block);

    char * buffer = pool.take();
    ASSERT_NE(buffer, nullptr);

    for (size_t i = 0; i < block; ++i)
    {
        buffer[i] = static_cast<char>(i & 0xFF);
    }
    for (size_t i = 0; i < block; ++i)
    {
        ASSERT_EQ(buffer[i], static_cast<char>(i & 0xFF)) << "byte " << i;
    }
}

// An empty pool is legal - it is what a buffered engine builds - and must not allocate or crash.
TEST(ScratchPool, Empty_Pool_Is_Legal)
{
    ScratchPool pool(0, 4096);

    EXPECT_EQ(pool.free_count(), 0u);
    EXPECT_EQ(pool.take(), nullptr);
}

}; // namespace runai::llm::streamer::common::posix_io
