#include "streamer/impl/streamer/streamer.h"

#include <unistd.h>

#include <gtest/gtest.h>

#include <sys/syscall.h>
#include <unistd.h>

#include <cstring>
#include <atomic>
#include <string>
#include <utility>
#include <vector>
#include <set>

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/fd/fd.h"
#include "utils/thread/thread.h"
#include "utils/temp/env/env.h"
#include "utils/temp/file/file.h"

namespace runai::llm::streamer::impl
{

namespace
{

// Read the next response off the persistent responder (blocking), with its submission_done flag. There is no
// finish-on-drain in the multi-request API: a submission is complete when its last response carries submission_done ==
// true (the equivalent of the old FinishedError-on-drain).
struct Received
{
    common::Response response;
    bool submission_done = false;
};

Received recv(Streamer & streamer)
{
    // common::Response has no default constructor, so build it first and aggregate-init Received (no
    // default-construct-then-assign).
    bool submission_done = false;
    auto response = streamer.response(0, submission_done);
    return Received{ response, submission_done };
}

// The range indices a submission of n ranges owes: exactly {0, 1, ... n-1}. Compared as a SET, because a
// count (received.size() == n) also passes when a range is answered twice and another dropped, or when an
// index is out of range entirely.
std::set<unsigned> range_indices(unsigned n)
{
    std::set<unsigned> indices;
    for (unsigned i = 0; i < n; ++i)
    {
        indices.insert(i);
    }
    return indices;
}


// The kernel directly - not IoUringProbe and not StrategyResolver, both of which are on the path
// under test here.
bool ring_works()
{
    struct params_stub { char opaque[512]; } params;
    std::memset(&params, 0, sizeof(params));

    const int fd = ::syscall(425 /* __NR_io_uring_setup */, 8, &params);
    if (fd < 0)
    {
        return false;
    }
    ::close(fd);
    return true;
}

// short wait used to assert a fresh/empty responder delivers nothing (it times out rather than blocking)
constexpr unsigned EMPTY_WAIT_MS = 50;

} // namespace

TEST(Creation, Default)
{
    Config config;
    Streamer streamer(config);
    // fresh streamer, no request: the persistent responder has nothing, so a timed wait times out (it does
    // NOT report FinishedError - that is teardown-only in the multi-request API)
    bool submission_done = false;
    auto r = streamer.response(EMPTY_WAIT_MS, submission_done);
    EXPECT_EQ(r.ret, common::ResponseCode::TimedOut);
    EXPECT_FALSE(submission_done);
}

TEST(Creation, Sanity)
{
    Streamer streamer;
    bool submission_done = false;
    auto r = streamer.response(EMPTY_WAIT_MS, submission_done);
    EXPECT_EQ(r.ret, common::ResponseCode::TimedOut);
    EXPECT_FALSE(submission_done);
}

// A read failure is attributable to its file, so it must NOT be reported as UnknownError. UnknownError is
// reserved for unrecoverable conditions (corruption, out of memory) and tells the caller to abort
// everything - reporting it for one file's I/O error would poison every other in-flight submission.
//
// A directory is the cheapest real read failure available: open(O_RDONLY) succeeds on it, and the read
// then fails with EISDIR - no fault injection needed.
// S6a's whole point: a real submission served by the io_uring engine rather than the synchronous
// reader, with the same bytes out.
//
// The strategy assertion is what makes this test mean anything. Both paths return identical data, so
// checking only the bytes would pass just as well if the request quietly went to the threadpool.
TEST(Async, ReadsThroughIoUringWhenResolvedToIt)
{
    utils::temp::Env strategy(std::string("RUNAI_STREAMER_FS_STRATEGY"), std::string("io_uring_buffered,sync_buffered"));

    const auto data = utils::random::buffer(1 << 20);
    utils::temp::File file(data);

    const unsigned ranges = 8;
    const size_t range_size = data.size() / ranges;

    std::vector<char> dst(data.size());
    std::vector<FileRanges> request(1);
    request[0].path = file.path;
    for (unsigned i = 0; i < ranges; ++i)
    {
        request[0].ranges.push_back(ReadRange{ i * range_size, range_size, dst.data() + i * range_size });
    }

    Streamer streamer;   // reads RUNAI_STREAMER_FS_STRATEGY through Config

    SubmissionId submission_id = 0;
    ASSERT_EQ(streamer.async_request(request, &submission_id), common::ResponseCode::Success);

    std::set<unsigned> seen;
    for (unsigned i = 0; i < ranges; ++i)
    {
        const auto received = recv(streamer);
        EXPECT_EQ(received.response.ret, common::ResponseCode::Success);
        EXPECT_EQ(received.response.submission_id, submission_id);
        seen.insert(received.response.index);
    }
    EXPECT_EQ(seen, range_indices(ranges));

    // Which path actually served it. On a host without a ring the list falls through to
    // sync_buffered, and this test then covers the fallback instead - still a real assertion.
    const bool expect_async = ring_works();

    EXPECT_EQ(streamer.fs_strategy(),
              expect_async ? common::posix_io::Strategy::IoUringBuffered
                           : common::posix_io::Strategy::SyncBuffered);

    // What was CHOSEN above; what was USED here. Without this, a dispatch that ignored the resolved
    // strategy and sent everything to the threadpool would pass every assertion in this test.
    EXPECT_EQ(streamer.async_pool_used(), expect_async);

    EXPECT_EQ(std::vector<char>(dst.begin(), dst.end()),
              std::vector<char>(data.begin(), data.end()));
}

// The default must stay the synchronous reader until the A/B says otherwise. A default that drifted
// to io_uring would decide by omission what the measurement is meant to decide.
TEST(Async, DefaultStrategyIsSynchronous)
{
    utils::temp::UnsetEnv strategy(std::string("RUNAI_STREAMER_FS_STRATEGY"));

    const auto data = utils::random::buffer(4096);
    utils::temp::File file(data);

    std::vector<char> dst(data.size());
    std::vector<FileRanges> request(1);
    request[0].path = file.path;
    request[0].ranges.push_back(ReadRange{ 0, data.size(), dst.data() });

    Streamer streamer;

    SubmissionId submission_id = 0;
    ASSERT_EQ(streamer.async_request(request, &submission_id), common::ResponseCode::Success);
    EXPECT_EQ(recv(streamer).response.ret, common::ResponseCode::Success);

    EXPECT_EQ(streamer.fs_strategy(), common::posix_io::Strategy::SyncBuffered);
    EXPECT_FALSE(streamer.async_pool_used()) << "the default must not build a ring or a thread";
}

// An unservable list is an error, not a quiet fall-through to the synchronous reader - and it must
// fail the REQUEST, since that is the only place the caller can see it.
TEST(Async, UnservableStrategyFailsTheRequest)
{
    utils::temp::Env strategy(std::string("RUNAI_STREAMER_FS_STRATEGY"), std::string("libaio_direct"));

    const auto data = utils::random::buffer(4096);
    utils::temp::File file(data);

    std::vector<char> dst(data.size());
    std::vector<FileRanges> request(1);
    request[0].path = file.path;
    request[0].ranges.push_back(ReadRange{ 0, data.size(), dst.data() });

    Streamer streamer;

    SubmissionId submission_id = 123;   // must be cleared, so a stale id cannot be mistaken for a real one

    // NOT merely "!= Success". Ignoring the resolution failure also produces a non-Success code -
    // dispatch asserts on the unresolved strategy and the catch block reports UnknownError - but that
    // happens AFTER the submission is registered and its responses counted, and UnknownError tells
    // the caller to abort everything rather than just this request. The specific code is what
    // separates a clean refusal from a late collapse.
    EXPECT_EQ(streamer.async_request(request, &submission_id), common::ResponseCode::UnsupportedBackendMix);

    // Nothing was committed: no id was minted, so the caller owes nothing and nothing owes it.
    EXPECT_EQ(submission_id, 0u);

    // And no response is waiting - a submission that was never accepted must not have produced one.
    bool done = false;
    EXPECT_EQ(streamer.response(EMPTY_WAIT_MS, done).ret, common::ResponseCode::TimedOut);
}

TEST(Async, ReadFailureIsAttributableNotUnknown)
{
    utils::temp::Dir dir;

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    const size_t size = 128;
    std::vector<unsigned char> dst(size);

    std::vector<FileRanges> request;
    request.push_back(FileRanges{ dir.path, { ReadRange{ 0, size, dst.data() } } });

    EXPECT_EQ(streamer.async_request(request), common::ResponseCode::Success);

    bool done = false;
    const auto response = streamer.response(60000, done);

    EXPECT_NE(response.ret, common::ResponseCode::Success);
    EXPECT_NE(response.ret, common::ResponseCode::UnknownError)
        << "a per-file read failure must not tell the caller to abort everything";
    // the submission still completes - the caller's buffer is released only on this flag
    EXPECT_TRUE(done);
}

TEST(Async, ResponseBlocksUntilResponseArrives)
{
    // response(0) on a streamer with no ready response BLOCKS until one arrives (persistent responder, no
    // finish-on-drain). A background consumer waits; the main thread submits a read; the consumer receives it.
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes = { size };

    std::atomic<bool> got_response{false};
    common::ResponseCode ret = common::ResponseCode::UnknownError;
    utils::Thread consumer([&]()
    {
        bool submission_done = false;
        auto r = streamer.response(0, submission_done);   // blocks until the read below completes
        ret = r.ret;
        got_response = true;
    });

    // let the consumer reach the blocking wait, then confirm it is still blocked (nothing submitted yet)
    ::usleep(50 * 1000);
    EXPECT_FALSE(got_response.load());

    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst.data(), 1, sizes.data()), common::ResponseCode::Success);

    consumer.join();
    EXPECT_TRUE(got_response.load());
    EXPECT_EQ(ret, common::ResponseCode::Success);
}

TEST(Sync, Sanity)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(2, 30), utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    std::vector<unsigned char> v(size);
    auto result = streamer.sync_read(file.path, 0, size, v.data());
    EXPECT_EQ(result, common::ResponseCode::Success);

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(v[i], expected[i]);
        if (v[i] != expected[i])
        {
            break;
        }
    }
}

TEST(Sync, File_Not_Found_Error)
{
    auto size = utils::random::number(100, 1000);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(2, 30), utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);
    std::vector<char> v(size);
    auto r = streamer.sync_read(utils::random::string(), 0, size, v.data());
    EXPECT_EQ(r, common::ResponseCode::FileAccessError);
}

TEST(Sync, End_Of_File_Error)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size / 2);
    utils::temp::File file(data);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(2, 30), utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    std::vector<char> v(size);

    for (size_t file_offset : {0UL, utils::random::number<size_t>(size, 100 * size)})
    {
        auto r = streamer.sync_read(file.path, file_offset, size, v.data());
        EXPECT_EQ(r, common::ResponseCode::EofError);
    }

    for (size_t file_offset : {utils::random::number<size_t>(size/2, size), utils::random::number<size_t>(size, 100 * size)})
    {
        auto r = streamer.sync_read(file.path, file_offset, utils::random::number<size_t>(1, size/2), v.data());
        EXPECT_EQ(r, common::ResponseCode::EofError);
    }
}

TEST(Sync, Offset)
{
    auto size = 1024;
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    auto offset_end = utils::random::number<size_t>(2, size);
    auto offset_start = utils::random::number<size_t>(offset_end - 1);
    auto size_to_read = offset_end - offset_start;

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);


    std::vector<unsigned char> v(size_to_read);
    {
        Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
        Streamer streamer(config);

        auto r = streamer.sync_read(file.path, offset_start, size_to_read, v.data());
        EXPECT_EQ(r, common::ResponseCode::Success);
    }

    for (size_t i = 0; i < size_to_read; ++i)
    {
        EXPECT_EQ(v[i], expected[i + offset_start]);
        if (v[i] != expected[i + offset_start])
        {
            break;
        }
    }
}

TEST(Async, Sanity)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst.data(), 1, sizes.data()), common::ResponseCode::Success);
    auto received = recv(streamer);
    EXPECT_EQ(received.response.ret, common::ResponseCode::Success);
    EXPECT_EQ(received.response.index, 0);
    EXPECT_TRUE(received.submission_done);   // single-range submission: this is its last response

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst[i], expected[i]);
        if (dst[i] != expected[i])
        {
            break;
        }
    }
}

TEST(Async, Requests)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);
    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);


    std::vector<unsigned char> dst(size);
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst.data(), num_chunks, chunks.data()), common::ResponseCode::Success);

    // wait for all the requests to finish
    std::set<int> expected_responses;

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        expected_responses.insert(i);
    }

    unsigned done_count = 0;
    for (unsigned i = 0; i < num_chunks; ++i)
    {
        const auto received = recv(streamer);
        EXPECT_EQ(received.response.ret, common::ResponseCode::Success);
        LOG(SPAM) << "received response of request " << received.response.index;
        EXPECT_EQ(expected_responses.count(received.response.index), 1);
        expected_responses.erase(received.response.index);
        if (received.submission_done) ++done_count;
    }

    EXPECT_TRUE(expected_responses.empty());
    EXPECT_EQ(done_count, 1u);   // submission_done fires exactly once, on the submission's last response

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst[i], expected[i]);
        if (dst[i] != expected[i])
        {
            break;
        }
    }
}

TEST(Async, File_Not_Found_Error)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);
    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    std::vector<char> dst(size);
    EXPECT_EQ(streamer.async_read(utils::random::string(), 0, size, dst.data(), num_chunks, chunks.data()), common::ResponseCode::Success);

    unsigned done_count = 0;
    for (unsigned i = 0; i < num_chunks; ++i)
    {
        const auto received = recv(streamer);
        EXPECT_EQ(received.response.ret, common::ResponseCode::FileAccessError);
        if (received.submission_done) ++done_count;
    }
    EXPECT_EQ(done_count, 1u);   // the failed submission still completes: submission_done on its last response
}

TEST(Async, End_Of_File_Error)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    auto chunks = utils::random::chunks(size, num_chunks);

    // write data just for the first chunks

    const auto chunk_size = utils::random::number<size_t>(10, size - 1);
    const auto block_size = utils::random::number<size_t>(1, chunk_size);

    LOG(DEBUG) << "writing only " << chunk_size << " bytes";
    const auto data = utils::random::buffer(chunk_size);
    utils::temp::File file(data);

    Config config(utils::random::number(1, 20), chunk_size, block_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    std::vector<char> dst(size);


    auto request_ret = streamer.async_read(file.path, 0, size, dst.data(), num_chunks, chunks.data());

    EXPECT_EQ(request_ret, common::ResponseCode::Success);

    // wait for all the requests to finish

    unsigned count_successful = 0;
    unsigned done_count = 0;
    for (unsigned i = 0; i < num_chunks; ++i)
    {
        const auto received = recv(streamer);
        LOG(SPAM) << "received response of request " << received.response.index << " : " << received.response.ret;
        if (received.response.ret == common::ResponseCode::Success)
        {
            ++count_successful;
        }
        else
        {
            EXPECT_EQ(received.response.ret, common::ResponseCode::EofError);
        }
        if (received.submission_done) ++done_count;
    }
    EXPECT_LT(count_successful, num_chunks);
    EXPECT_EQ(done_count, 1u);   // submission completes once its last sub-range lands
}

TEST(Async, Zero_Requests)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 10), utils::random::number(1, 10), chunk_size, bulk_size, false /* do not enforce minimum */);


    Streamer streamer(config);

    std::vector<char> dst(size);

    // A submission with no ranges is accepted, not rejected: it simply reads nothing. Empties are
    // absorbed rather than refused, so there is no InvalidParameterError / EmptyRequestError here any
    // more. It is not registered either, so it owes no responses and there is nothing to receive.
    EXPECT_EQ(streamer.async_read(utils::random::string(), 0, size, dst.data(), 0, chunks.data()), common::ResponseCode::Success);
}

TEST(Async, Zero_Bytes_To_Read)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);


    Streamer streamer(config);

    std::vector<char> dst(size);

    // Zero ranges is legal and reads nothing. (The path is random and does not exist, which does not
    // matter: with no ranges nothing ever reaches storage.)
    EXPECT_EQ(streamer.async_read(utils::random::string(), 0, 0, dst.data(), 0, chunks.data()), common::ResponseCode::Success);
}

// Replaces the second branch of the old Zero_Bytes_To_Read_Error, which asserted that a zero total with
// non-zero sub ranges was rejected. That contradiction is unrepresentable now - the ranges define the
// span, there is no separate total to disagree with - so this asserts what the new contract says
// instead: zero sized ranges are accepted AND answered, one response each.
TEST(Async, Zero_Sized_Ranges)
{
    const unsigned num_ranges = utils::random::number(1, 20);
    std::vector<size_t> zero_chunks(num_ranges, 0);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    // a real file: a zero sized transfer still opens its file, it just reads nothing from it
    const auto data = utils::random::buffer(utils::random::number(1, 100));
    utils::temp::File file(data);

    std::vector<char> dst(1);
    EXPECT_EQ(streamer.async_read(file.path, 0, 0, dst.data(), num_ranges, zero_chunks.data()),
              common::ResponseCode::Success);

    std::set<unsigned> received;
    for (unsigned i = 0; i < num_ranges; ++i)
    {
        bool done = false;
        const auto r = streamer.response(60000, done);
        EXPECT_EQ(r.ret, common::ResponseCode::Success);
        received.insert(r.index);
        EXPECT_EQ(done, i + 1 == num_ranges);   // completion lands on the last range
    }

    EXPECT_EQ(received, range_indices(num_ranges));
}

TEST(Async, ConcurrentRequests)
{
    // Multiple submissions are now accepted concurrently (no BusyError). Each is demuxed on the
    // shared persistent responder and both are delivered, each completing (submission_done) on its
    // single response.
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);


    Streamer streamer(config);

    // disjoint destination buffers, one per concurrent submission
    std::vector<unsigned char> dst1(size), dst2(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);

    // both requests are accepted - the second does NOT return BusyError
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst1.data(), 1, sizes.data()), common::ResponseCode::Success);
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst2.data(), 1, sizes.data()), common::ResponseCode::Success);

    // both submissions are delivered; each is single-range, so each response is its submission's last
    std::set<unsigned> completed;
    for (int i = 0; i < 2; ++i)
    {
        const auto received = recv(streamer);
        EXPECT_EQ(received.response.ret, common::ResponseCode::Success);
        EXPECT_TRUE(received.submission_done);
        completed.insert(received.response.submission_id);
    }
    EXPECT_EQ(completed.size(), 2u);   // two distinct submissions, each ended

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst1[i], expected[i]);
        EXPECT_EQ(dst2[i], expected[i]);
        if (dst1[i] != expected[i] || dst2[i] != expected[i])
        {
            break;
        }
    }
}

// Previously this asserted the Assigner's "Input vector sizes mismatch" throw - it passed two paths but
// one entry in every other vector. Mismatched lengths are unrepresentable now (a request is a list of
// FileRanges), so it asserts the surviving property of a submission whose paths disagree: mixing two
// object-storage plugins is rejected up front. A different pair than MixedObjectPluginsRejected below.
TEST(AsyncRequest, InvalidScheme)
{
    const auto size = utils::random::number(100, 1000);
    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    std::vector<unsigned char> dst0(size);
    std::vector<unsigned char> dst1(size);

    std::vector<FileRanges> request;
    request.push_back(FileRanges{ "s3://s3-bucket/file-01.txt", { ReadRange{ 0, static_cast<size_t>(size), dst0.data() } } });
    request.push_back(FileRanges{ "az://az-account/file-02.txt", { ReadRange{ 0, static_cast<size_t>(size), dst1.data() } } });

    EXPECT_EQ(streamer.async_request(request), common::ResponseCode::UnsupportedBackendMix);
}

TEST(AsyncRequest, MixedObjectPluginsRejected)
{
    const auto size = utils::random::number(100, 1000);
    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    std::vector<unsigned char> dst0(size);
    std::vector<unsigned char> dst1(size);

    // a single submission that mixes two object-storage plugins (s3 + gcs) is rejected up front,
    // before any dispatch or plugin load
    std::vector<FileRanges> request;
    request.push_back(FileRanges{ "s3://bucket/a.txt", { ReadRange{ 0, static_cast<size_t>(size), dst0.data() } } });
    request.push_back(FileRanges{ "gs://bucket/b.txt", { ReadRange{ 0, static_cast<size_t>(size), dst1.data() } } });

    EXPECT_EQ(streamer.async_request(request), common::ResponseCode::UnsupportedBackendMix);
}

// A submission must pick ONE backend kind. The streamer serves both across submissions (see
// FilesystemAndObjectStorageSubmissionsCoexist), but within a submission the Assigner divides the work
// with a single backend's worker count and block size, and a workload has to be homogeneous to be routed.
// Rejecting up front replaces a slice-dependent outcome: without the check, this is InvalidParameterError
// when both kinds land in one workload and silently accepted with the wrong block size when they do not.
TEST(AsyncRequest, MixedFilesystemAndObjectStorageRejected)
{
    const auto size = utils::random::number(100, 1000);
    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    Streamer streamer(config);

    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    std::vector<unsigned char> dst0(size);
    std::vector<unsigned char> dst1(size);

    // both orders: the check must not depend on which kind is seen first (a first-file test would pass
    // one of these by accident)
    {
        std::vector<FileRanges> request;
        request.push_back(FileRanges{ file.path, { ReadRange{ 0, static_cast<size_t>(size), dst0.data() } } });
        request.push_back(FileRanges{ "s3://bucket/a.txt", { ReadRange{ 0, static_cast<size_t>(size), dst1.data() } } });

        EXPECT_EQ(streamer.async_request(request), common::ResponseCode::UnsupportedBackendMix);
    }
    {
        std::vector<FileRanges> request;
        request.push_back(FileRanges{ "s3://bucket/a.txt", { ReadRange{ 0, static_cast<size_t>(size), dst0.data() } } });
        request.push_back(FileRanges{ file.path, { ReadRange{ 0, static_cast<size_t>(size), dst1.data() } } });

        EXPECT_EQ(streamer.async_request(request), common::ResponseCode::UnsupportedBackendMix);
    }
}

// A file with no ranges reaches no storage (verify_requests accepts it deliberately, and it yields no
// transfer), so it must take no part in backend selection.
TEST(AsyncRequest, FilesWithoutRangesDoNotSelectTheBackend)
{
    const auto size = utils::random::number(100, 1000);
    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    {
        // An empty object-storage entry must not lock the streamer's plugin: that submission reads
        // nothing, so a later submission using a DIFFERENT plugin is still legitimate. Both submissions
        // here are empty, so neither reaches a pool or loads a plugin - the lock alone is under test.
        Streamer streamer(config);

        std::vector<FileRanges> s3_only;
        s3_only.push_back(FileRanges{ "s3://bucket/empty.txt", {} });
        EXPECT_EQ(streamer.async_request(s3_only), common::ResponseCode::Success);

        std::vector<FileRanges> gcs_only;
        gcs_only.push_back(FileRanges{ "gs://bucket/empty.txt", {} });
        EXPECT_EQ(streamer.async_request(gcs_only), common::ResponseCode::Success);
    }

    {
        // A filesystem submission carrying an empty object-storage entry is NOT a mixed submission: the
        // empty entry contributes no batch, so every workload is still filesystem. It must also not
        // select the object-storage worker count and block size for the assignment.
        Streamer streamer(config);

        std::vector<unsigned char> dst(size);
        std::vector<FileRanges> request;
        request.push_back(FileRanges{ "s3://bucket/empty.txt", {} });
        request.push_back(FileRanges{ file.path, { ReadRange{ 0, static_cast<size_t>(size), dst.data() } } });

        EXPECT_EQ(streamer.async_request(request), common::ResponseCode::Success);

        // drain the single range and check it really read the filesystem file
        bool done = false;
        const auto response = streamer.response(60000, done);
        EXPECT_EQ(response.ret, common::ResponseCode::Success);
        EXPECT_EQ(dst, std::vector<unsigned char>(data.begin(), data.end()));
    }
}

namespace
{

std::set<std::string> paths_of(const std::vector<std::pair<std::string, size_t>> & entries)
{
    std::set<std::string> result;
    for (const auto & entry : entries)
    {
        result.insert(entry.first);
    }
    return result;
}

} // namespace

TEST(ListFiles, FilesystemBasicListingAndSizes)
{
    Streamer streamer;

    utils::temp::Dir dir;
    const auto data_a = utils::random::buffer(utils::random::number(1, 1000));
    const auto data_b = utils::random::buffer(utils::random::number(1, 1000));
    utils::temp::File a(dir.path, "a.bin", data_a);
    utils::temp::File b(dir.path, "b.bin", data_b);

    const auto entries = streamer.list_files(dir.path, true, {}, {});

    EXPECT_EQ(entries.size(), 2u);
    bool found_a = false;
    bool found_b = false;
    for (const auto & entry : entries)
    {
        if (entry.first == a.path) { EXPECT_EQ(entry.second, data_a.size()); found_a = true; }
        if (entry.first == b.path) { EXPECT_EQ(entry.second, data_b.size()); found_b = true; }
    }
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
}

TEST(ListFiles, FilesystemRecursive)
{
    Streamer streamer;

    utils::temp::Dir dir;
    utils::temp::File root_file(dir.path, "root.bin", utils::random::buffer(10));
    utils::temp::Dir sub(dir.path, "subdir");
    utils::temp::File nested(sub.path, "nested.bin", utils::random::buffer(10));

    const auto recursive = paths_of(streamer.list_files(dir.path, true, {}, {}));
    const auto non_recursive = paths_of(streamer.list_files(dir.path, false, {}, {}));

    EXPECT_TRUE(recursive.count(root_file.path));
    EXPECT_TRUE(recursive.count(nested.path));

    EXPECT_TRUE(non_recursive.count(root_file.path));
    EXPECT_FALSE(non_recursive.count(nested.path));
}

TEST(ListFiles, FilesystemAllowPattern)
{
    Streamer streamer;

    utils::temp::Dir dir;
    utils::temp::File st(dir.path, "model.safetensors", utils::random::buffer(10));
    utils::temp::File js(dir.path, "config.json", utils::random::buffer(10));

    const auto paths = paths_of(streamer.list_files(dir.path, true, {"*.safetensors"}, {}));

    EXPECT_TRUE(paths.count(st.path));
    EXPECT_FALSE(paths.count(js.path));
}

TEST(ListFiles, FilesystemIgnorePattern)
{
    Streamer streamer;

    utils::temp::Dir dir;
    utils::temp::File st(dir.path, "model.safetensors", utils::random::buffer(10));
    utils::temp::File js(dir.path, "config.json", utils::random::buffer(10));

    const auto paths = paths_of(streamer.list_files(dir.path, true, {}, {"*.json"}));

    EXPECT_TRUE(paths.count(st.path));
    EXPECT_FALSE(paths.count(js.path));
}

TEST(ListFiles, FilesystemNonExistentPathThrows)
{
    Streamer streamer;

    const std::string missing = "./" + utils::random::string() + "/" + utils::random::string();
    try
    {
        streamer.list_files(missing, true, {}, {});
        FAIL() << "expected an exception for a non-existent path";
    }
    catch (const common::Exception & e)
    {
        EXPECT_EQ(e.error(), common::ResponseCode::FileAccessError);
    }
}

TEST(ListFiles, FilesystemEmptyDirectory)
{
    Streamer streamer;

    utils::temp::Dir dir;

    const auto entries = streamer.list_files(dir.path, true, {}, {});
    EXPECT_TRUE(entries.empty());
}

TEST(Async, Scattered_Ranges_And_Destinations)
{
    // The point of the range API: ranges need not be contiguous in the file, need not be ordered, and
    // need not be written to adjacent memory. Every other data test here goes through async_read, which
    // tiles one span of the file into one buffer - so none of them would notice if offsets or
    // destinations were silently paired by position instead of being honoured per range.
    const size_t size = 1000;
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);
    const auto expected = utils::Fd::read(file.path);
    ASSERT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size,
                  utils::random::number<size_t>(1, chunk_size), false /* do not enforce minimum */);
    Streamer streamer(config);

    // deliberately: descending file order, gaps between ranges, a zero-sized range, and two ranges
    // reading the SAME source bytes (source overlap is legal - only destinations must not overlap)
    std::vector<std::pair<size_t, size_t>> ranges =
    {
        { 700, 120 },
        {  50, 200 },
        { 400,   0 },
        { 700, 120 },
        { 900, 100 },
    };

    // Then a random number of random ranges on top: the five above are the shapes worth naming, but their
    // count is arbitrary, and a fixed count is one a position bug can fit by accident.
    const unsigned extra = utils::random::number(0, 15);
    for (unsigned i = 0; i < extra; ++i)
    {
        const size_t offset = utils::random::number<size_t>(0, size - 1);
        ranges.emplace_back(offset, utils::random::number<size_t>(0, size - offset));
    }

    // each destination is its OWN allocation, not an offset into a shared buffer: this is what proves a
    // destination need not belong to any single buffer. The extra byte is a guard against an over-long write.
    std::vector<std::vector<unsigned char>> dsts;
    for (const auto & range : ranges)
    {
        dsts.emplace_back(range.second + 1, 0xAB);
    }

    FileRanges file_ranges;
    file_ranges.path = file.path;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        file_ranges.ranges.push_back(ReadRange{ ranges[i].first, ranges[i].second, dsts[i].data() });
    }

    std::vector<FileRanges> request{ file_ranges };
    SubmissionId submission_id = 0;
    EXPECT_EQ(streamer.async_request(request, &submission_id), common::ResponseCode::Success);

    std::set<unsigned> received;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        const auto r = recv(streamer);
        EXPECT_EQ(r.response.ret, common::ResponseCode::Success);
        EXPECT_EQ(r.response.file_index, 0u);
        received.insert(r.response.index);
        EXPECT_EQ(r.submission_done, i + 1 == ranges.size());
    }

    // exactly one response per range, indexed within the file - a dropped or duplicated zero-sized
    // range would shift every later index
    EXPECT_EQ(received, range_indices(ranges.size()));

    for (size_t i = 0; i < ranges.size(); ++i)
    {
        const auto offset = ranges[i].first;
        const auto length = ranges[i].second;
        for (size_t j = 0; j < length; ++j)
        {
            ASSERT_EQ(dsts[i][j], expected[offset + j])
                << "range " << i << " (offset " << offset << " size " << length << ") differs at byte " << j;
        }
        EXPECT_EQ(dsts[i][length], 0xAB) << "range " << i << " wrote past the end of its destination";
    }
}

}; // namespace runai::llm::streamer::impl
