#include "common/posix_io/completion_mapper/completion_mapper.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <string>
#include <vector>

namespace runai::llm::streamer::common::posix_io
{

namespace
{

FileRef buffered() { return FileRef{ 7, false }; }
FileRef direct()   { return FileRef{ 7, true  }; }

} // namespace

// A byte count, including zero. Whether zero means EOF depends on what was asked and what has already
// arrived, which only the caller knows.
TEST(CompletionMapper, Non_Negative_Is_Success)
{
    EXPECT_EQ(map_completion(4096, buffered()), ResponseCode::Success);
    EXPECT_EQ(map_completion(1, buffered()), ResponseCode::Success);
    EXPECT_EQ(map_completion(0, buffered()), ResponseCode::Success);
    EXPECT_EQ(map_completion(0, direct()), ResponseCode::Success);
}

// Attributable to this file. Other files, and other submissions, carry on.
TEST(CompletionMapper, Storage_Errors_Are_Per_File)
{
    for (const long err : { EIO, ENXIO, ESTALE, ETIMEDOUT, ECONNRESET, EREMOTEIO })
    {
        EXPECT_EQ(map_completion(-err, buffered()), ResponseCode::FileAccessError) << "errno " << err;
        EXPECT_EQ(map_completion(-err, direct()), ResponseCode::FileAccessError) << "errno " << err;
    }
}

// Teardown, not a storage fault. Reporting a cancelled read as FileAccessError sends an operator to
// investigate the wrong system.
TEST(CompletionMapper, Cancelled_Is_Finished_Not_File_Access)
{
    EXPECT_EQ(map_completion(-ECANCELED, buffered()), ResponseCode::FinishedError);
    EXPECT_EQ(map_completion(-ECANCELED, direct()), ResponseCode::FinishedError);
}

// EINVAL means our alignment contract broke - but only on a direct fd. On a buffered one it means
// something else and must not be reported as an internal bug.
TEST(CompletionMapper, Einval_Is_Mode_Aware)
{
    EXPECT_TRUE(is_internal_error(-EINVAL, direct()));
    EXPECT_FALSE(is_internal_error(-EINVAL, buffered()));

    // Buffered EINVAL falls through to the default branch rather than asserting.
    EXPECT_EQ(map_completion(-EINVAL, buffered()), ResponseCode::UnknownError);
}

// The engine never opens or closes anything, so a bad fd can only be our own bookkeeping.
TEST(CompletionMapper, Our_Bugs_Are_Not_Storage_Errors)
{
    for (const auto & file : { buffered(), direct() })
    {
        EXPECT_TRUE(is_internal_error(-EFAULT, file));
        EXPECT_TRUE(is_internal_error(-EBADF, file));

        EXPECT_FALSE(is_internal_error(-EIO, file));
        EXPECT_FALSE(is_internal_error(-ECANCELED, file));
        EXPECT_FALSE(is_internal_error(4096, file));
    }
}

// The mapper must RETURN for an internal error, not throw or abort: wait_for_completions is declared
// to return a code, and a library inside a serving stack must not take its host down. Testing the
// predicate alone missed this - the mapper asserted here, and ASSERT is fatal in every build.
TEST(CompletionMapper, Internal_Errors_Return_Unknown_Rather_Than_Throwing)
{
    EXPECT_EQ(map_completion(-EFAULT, buffered()), ResponseCode::UnknownError);
    EXPECT_EQ(map_completion(-EBADF, buffered()), ResponseCode::UnknownError);
    EXPECT_EQ(map_completion(-EFAULT, direct()), ResponseCode::UnknownError);
    EXPECT_EQ(map_completion(-EINVAL, direct()), ResponseCode::UnknownError);

    EXPECT_NO_THROW(map_completion(-EINVAL, direct()));
    EXPECT_NO_THROW(map_completion(-EBADF, direct()));
}

// Unrecognised errnos are UnknownError, never FileAccessError - guessing "the storage failed" for
// something we do not understand is the wrong direction to be wrong in.
TEST(CompletionMapper, Unknown_Errno)
{
    EXPECT_EQ(map_completion(-ENOSPC, buffered()), ResponseCode::UnknownError);
    EXPECT_EQ(map_completion(-EPERM, buffered()), ResponseCode::UnknownError);
}

// EINTR is unreachable as a completion - libaio surfaces it from the wait, not the result - so there
// is deliberately no row for it. Asserted so that adding one is a conscious act.
TEST(CompletionMapper, Eintr_Has_No_Special_Row)
{
    EXPECT_EQ(map_completion(-EINTR, buffered()), ResponseCode::UnknownError);
}

// The message must name WHICH constraint failed. A bare EINVAL sends someone to the filesystem when
// the fault is ours.
TEST(CompletionMapper, Alignment_Diagnosis_Names_The_Broken_Constraint)
{
    Limits limits;
    limits.offset_alignment = 512;
    limits.buffer_alignment = 512;

    alignas(512) char aligned[1024] = {};

    // Offset and length both aligned, buffer aligned - nothing marked bad.
    const auto clean = alignment_diagnosis(Requested{ 1024, 512, aligned }, limits);
    EXPECT_EQ(clean.find("BAD"), std::string::npos) << clean;

    // A length that is not a multiple of the block size. offset_alignment governs LENGTH as well as
    // offset, so this must be caught by it rather than silently passing.
    const auto bad_length = alignment_diagnosis(Requested{ 1024, 100, aligned }, limits);
    EXPECT_NE(bad_length.find("length=100"), std::string::npos) << bad_length;
    EXPECT_NE(bad_length.find("BAD"), std::string::npos) << bad_length;

    const auto bad_offset = alignment_diagnosis(Requested{ 1000, 512, aligned }, limits);
    EXPECT_NE(bad_offset.find("offset=1000"), std::string::npos) << bad_offset;
    EXPECT_NE(bad_offset.find("BAD"), std::string::npos) << bad_offset;

    const auto bad_buffer = alignment_diagnosis(Requested{ 1024, 512, aligned + 1 }, limits);
    EXPECT_NE(bad_buffer.find("BAD"), std::string::npos) << bad_buffer;
}

}; // namespace runai::llm::streamer::common::posix_io
