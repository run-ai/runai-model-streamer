#include "streamer/impl/config/fs_parallelism/fs_parallelism.h"

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "common/exception/exception.h"

namespace runai::llm::streamer::impl
{

// A plain number is the whole value, and stays the whole meaning of the variable for anyone who never
// needs per-type entries.
TEST(FsParallelism, A_Plain_Number_Applies_Everywhere)
{
    const auto parallelism = FsParallelism::parse("512");

    EXPECT_EQ(parallelism.default_value(), 512u);
    EXPECT_TRUE(parallelism.entries().empty());

    for (const auto * type : { "ext4", "nfs", "nfs4", "virtiofs", "overlay", "" })
    {
        EXPECT_EQ(parallelism.for_type(type), 512u) << type;
    }
}

TEST(FsParallelism, Type_Entries_Override_The_Default)
{
    const auto parallelism = FsParallelism::parse("512,nfs=64,virtiofs=256");

    EXPECT_EQ(parallelism.default_value(), 512u);
    ASSERT_EQ(parallelism.entries().size(), 2u);

    EXPECT_EQ(parallelism.for_type("nfs"), 64u);
    EXPECT_EQ(parallelism.for_type("virtiofs"), 256u);
    EXPECT_EQ(parallelism.for_type("ext4"), 512u) << "a type with no entry falls back to the default";
}

// The reason keys are prefixes: the kernel reports `nfs` for a v3 mount and `nfs4` for a v4 one, and a
// user should not have to know which they have.
TEST(FsParallelism, A_Key_Matches_As_A_Prefix)
{
    const auto parallelism = FsParallelism::parse("512,nfs=64,fuse=128");

    EXPECT_EQ(parallelism.for_type("nfs"), 64u);
    EXPECT_EQ(parallelism.for_type("nfs4"), 64u);
    EXPECT_EQ(parallelism.for_type("fuse.gvfsd-fuse"), 128u);

    EXPECT_EQ(parallelism.for_type("nf"), 512u) << "a shorter type is not matched by a longer key";
}

// First match, not longest match, and the caller is told which entry answered so it can log it. Written
// down because it is the one surprising case: `nfs4` mounts take the `nfs` entry when it is listed
// first, and writing the longer key first gives the other answer.
TEST(FsParallelism, The_First_Matching_Entry_Wins_And_Is_Reported)
{
    const auto first_shorter = FsParallelism::parse("512,nfs=64,nfs4=32");
    size_t matched = 0;

    EXPECT_EQ(first_shorter.for_type("nfs4", matched), 64u);
    EXPECT_EQ(matched, 0u) << "the caller must be able to name the entry that answered";

    const auto first_longer = FsParallelism::parse("512,nfs4=32,nfs=64");
    EXPECT_EQ(first_longer.for_type("nfs4", matched), 32u);
    EXPECT_EQ(matched, 0u);
    EXPECT_EQ(first_longer.for_type("nfs", matched), 64u);
    EXPECT_EQ(matched, 1u);
}

TEST(FsParallelism, The_Default_Reports_Itself_As_No_Entry)
{
    const auto parallelism = FsParallelism::parse("512,nfs=64");
    size_t matched = 0;

    EXPECT_EQ(parallelism.for_type("ext4", matched), 512u);
    EXPECT_EQ(matched, parallelism.entries().size()) << "out of range means the default answered";
}

TEST(FsParallelism, Whitespace_And_Case_Are_Ignored)
{
    const auto parallelism = FsParallelism::parse("  512 , NFS = 64 , VirtioFS=256 ");

    EXPECT_EQ(parallelism.default_value(), 512u);
    EXPECT_EQ(parallelism.for_type("nfs4"), 64u);
    EXPECT_EQ(parallelism.for_type("VIRTIOFS"), 256u);
}

// Every malformed value is refused at parse time, with the variable named. A value the user meant to
// set and mistyped must never read as "unset" - that is the rule the rest of the streamer's numeric
// variables already follow.
TEST(FsParallelism, Malformed_Values_Are_Refused)
{
    for (const auto * bad : {
            "",                     // nothing at all
            "   ",                  // nothing but spaces
            "nfs=64",               // no default, so a mount matching nothing has no answer
            "abc",                  // not a number
            "512,nfs",              // an entry with no value
            "512,=64",              // an entry with no type
            "512,nfs=",             // an entry with an empty value
            "512,nfs=abc",          // a non-numeric value
            "0",                    // a reader that admits nothing
            "512,nfs=0",            // the same, for one type
            "-1",                   // negative, which stoul would otherwise wrap
            "512,nfs=-4",
            "512,nfs=64,nfs=32",    // a repeat: both numbers were written on purpose
            "512,NFS=64,nfs=32",    // the same repeat, differently cased
            "99999999999999999999", // beyond unsigned
         })
    {
        EXPECT_THROW(FsParallelism::parse(bad), common::Exception) << "accepted: '" << bad << "'";
    }
}

TEST(FsParallelism, A_Constructed_Value_Behaves_Like_A_Plain_Number)
{
    const FsParallelism parallelism(64);

    EXPECT_EQ(parallelism.default_value(), 64u);
    EXPECT_TRUE(parallelism.entries().empty());
    EXPECT_EQ(parallelism.for_type("nfs"), 64u);
}

// The log line an operator reads back. It has to show the entries, or a value that parsed differently
// from what was intended is invisible.
TEST(FsParallelism, It_Prints_What_It_Parsed)
{
    std::ostringstream stream;
    stream << FsParallelism::parse("512,nfs=64,virtiofs=256");

    EXPECT_EQ(stream.str(), "512, nfs=64, virtiofs=256");
}

}; // namespace runai::llm::streamer::impl
