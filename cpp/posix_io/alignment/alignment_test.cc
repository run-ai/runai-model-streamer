#include "posix_io/alignment/alignment.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

namespace runai::llm::streamer::common::posix_io
{

namespace
{

Limits limits_of(size_t block)
{
    Limits limits;
    limits.offset_alignment = block;
    limits.buffer_alignment = block;
    return limits;
}

// An address with a known remainder, without needing a real allocation of that shape.
char * address_with_remainder(char * base, size_t block, size_t remainder)
{
    const auto current = reinterpret_cast<uintptr_t>(base) % block;
    return base + ((remainder + block - current) % block);
}

} // namespace

// One number covers all three kernel rules. The larger of the two is always safe: it only wastes.
TEST(Alignment, Block_Size_Takes_The_Larger)
{
    Limits limits;
    limits.offset_alignment = 512;
    limits.buffer_alignment = 4096;
    EXPECT_EQ(block_size(limits), 4096u);

    limits.offset_alignment = 4096;
    limits.buffer_alignment = 512;
    EXPECT_EQ(block_size(limits), 4096u);
}

// No rule means every read passes, rather than every read failing.
TEST(Alignment, No_Alignment_Rule_Means_One)
{
    Limits limits;
    limits.offset_alignment = 0;
    limits.buffer_alignment = 0;
    EXPECT_EQ(block_size(limits), 1u);

    char buffer[8] = {};
    EXPECT_TRUE(is_congruent(12345, buffer, block_size(limits)));
}

// The rule that decides everything. It is NOT "the address is aligned": what matters is that the
// address and the file offset are out of step by a whole number of blocks.
TEST(Alignment, Congruence_Compares_Remainders)
{
    constexpr size_t block = 4096;
    alignas(4096) char pool[3 * block] = {};

    // Aligned address, aligned offset - the easy case.
    EXPECT_TRUE(is_congruent(0, pool, block));
    EXPECT_TRUE(is_congruent(block, pool, block));

    // Aligned address, UNALIGNED offset. Every rule about addresses passes, and the read is still
    // impossible - this is the case the plain alignment checks miss.
    EXPECT_FALSE(is_congruent(1000, pool, block));

    // Unaligned address that is out of step by the SAME amount - this one works.
    EXPECT_TRUE(is_congruent(1000, address_with_remainder(pool, block, 1000), block));

    // Out of step by a different amount - does not.
    EXPECT_FALSE(is_congruent(1000, address_with_remainder(pool, block, 999), block));
}

// The answer must not depend on which of the two numbers is larger. A tiny address against a huge
// file offset is the normal case near the end of a large file.
//
// NOTE this does NOT prove the remainder form is needed. The subtraction form gives the same answer
// for a power-of-two block, because 2^64 is a multiple of it, so the wrap does not change the
// remainder. A mutation to the subtraction form passes this test, correctly.
TEST(Alignment, Congruence_Ignores_Which_Number_Is_Larger)
{
    constexpr size_t block = 4096;
    alignas(4096) char pool[block] = {};

    // A tiny file offset against a large address, and the reverse relation, must both be answered by
    // the remainders alone.
    EXPECT_TRUE(is_congruent(0, pool, block));
    EXPECT_FALSE(is_congruent(1, pool, block));
    EXPECT_TRUE(is_congruent(block * 1000, pool, block));
}

// The read moves down at the start and up at the end, and the address moves down by exactly the same
// amount. That is what keeps the file and the memory in step.
TEST(Alignment, Expand_Rounds_Both_Ends)
{
    constexpr size_t block = 4096;
    alignas(4096) char pool[4 * block] = {};

    // Region starts 1000 bytes into a block, and is 100 bytes long.
    char * const wanted = pool + block + 1000;
    const auto read = expand(block + 1000, 100, wanted, block);

    EXPECT_EQ(read.offset, block) << "start rounded down to the block below";
    EXPECT_EQ(read.head, 1000u) << "1000 unwanted bytes come first";
    EXPECT_EQ(read.buffer, wanted - 1000) << "the address moves down by the same 1000";
    EXPECT_EQ(read.bytesize, block) << "1000 + 100 rounded up to one block";

    // Everything the kernel checks must now pass.
    EXPECT_EQ(read.offset % block, 0u);
    EXPECT_EQ(read.bytesize % block, 0u);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(read.buffer) % block, 0u);

    // And the wanted bytes are still where the caller expects them.
    EXPECT_EQ(read.buffer + read.head, wanted);
}

// A read that is already aligned must not be changed. Rounding it anyway would read a whole extra
// block for nothing.
TEST(Alignment, Expand_Leaves_An_Aligned_Read_Alone)
{
    constexpr size_t block = 4096;
    alignas(4096) char pool[4 * block] = {};

    const auto read = expand(2 * block, 2 * block, pool, block);

    EXPECT_EQ(read.offset, 2 * block);
    EXPECT_EQ(read.bytesize, 2 * block);
    EXPECT_EQ(read.head, 0u);
    EXPECT_EQ(read.buffer, pool);
}

// A long region rounds up only what it has to, not to the next block after the whole length.
TEST(Alignment, Expand_Covers_The_Whole_Region)
{
    constexpr size_t block = 4096;
    alignas(4096) char pool[8 * block] = {};

    char * const wanted = pool + 100;
    const auto read = expand(100, 3 * block, wanted, block);

    EXPECT_EQ(read.head, 100u);
    // 100 head bytes + 3 blocks wanted = 3 blocks + 100, which needs a fourth block.
    EXPECT_EQ(read.bytesize, 4 * block);
    EXPECT_GE(read.bytesize, read.head + 3 * block) << "the read must cover every wanted byte";
}

// A bare EINVAL is the same answer for four different mistakes. The message must say which.
TEST(Alignment, Diagnosis_Names_The_Broken_Rule)
{
    const auto limits = limits_of(512);
    alignas(512) char aligned[1024] = {};

    const auto clean = alignment_diagnosis(Requested{ 1024, 512, aligned }, limits);
    EXPECT_EQ(clean.find("BAD"), std::string::npos) << clean;
    EXPECT_NE(clean.find("congruent=yes"), std::string::npos) << clean;

    // offset_alignment governs the LENGTH as well as the offset. Checking length against the buffer
    // rule instead gives a message where everything says ok and nothing is explained.
    const auto bad_length = alignment_diagnosis(Requested{ 1024, 100, aligned }, limits);
    EXPECT_NE(bad_length.find("length=100"), std::string::npos) << bad_length;
    EXPECT_NE(bad_length.find("BAD"), std::string::npos) << bad_length;

    const auto bad_offset = alignment_diagnosis(Requested{ 1000, 512, aligned }, limits);
    EXPECT_NE(bad_offset.find("offset=1000"), std::string::npos) << bad_offset;
    EXPECT_NE(bad_offset.find("BAD"), std::string::npos) << bad_offset;

    const auto bad_buffer = alignment_diagnosis(Requested{ 1024, 512, aligned + 1 }, limits);
    EXPECT_NE(bad_buffer.find("BAD"), std::string::npos) << bad_buffer;
}

// The case that the three alignment rules cannot see: every address rule passes, and the read is
// still impossible. Without this line in the message, the log would say nothing is wrong.
TEST(Alignment, Diagnosis_Reports_Failed_Congruence)
{
    const auto limits = limits_of(512);
    alignas(512) char aligned[1024] = {};

    // Offset aligned to 512 but not equal to the address remainder... offset 512 with an aligned
    // buffer IS congruent, so use an offset that breaks only congruence.
    const auto message = alignment_diagnosis(Requested{ 1000, 512, aligned }, limits);
    EXPECT_NE(message.find("congruent=no"), std::string::npos) << message;
}

}; // namespace runai::llm::streamer::common::posix_io
