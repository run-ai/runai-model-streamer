#include "common/posix_io/io_uring_engine/io_uring_engine.h"

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "utils/random/random.h"
#include "utils/temp/file/file.h"

namespace runai::llm::streamer::common::posix_io
{

namespace
{

#ifndef __NR_io_uring_setup
#define __NR_io_uring_setup 425
#endif

// Ask the kernel directly - not IoUringProbe, and not IoUringEngine.
//
// The gate must not be computed by anything it gates. If it read the probe, a probe that wrongly
// reported "unavailable" would skip every test that would have caught it, and the suite would stay
// green while the engine went entirely untested.
bool ring_works()
{
    struct io_uring_params params;
    std::memset(&params, 0, sizeof(params));

    const int fd = ::syscall(__NR_io_uring_setup, 8, &params);
    if (fd < 0)
    {
        return false;
    }

    ::close(fd);
    return true;
}

// Skipping is silent, and on a host that is supposed to have io_uring a silent skip is
// indistinguishable from a pass. RUNAI_STREAMER_REQUIRE_IO_URING says "io_uring works here", turning
// the skip into a failure. Set by `make -C cpp test_iouring`; unset in CI, which has no ring.
bool require_io_uring()
{
    const char * value = std::getenv("RUNAI_STREAMER_REQUIRE_IO_URING");
    return value != nullptr && std::string(value) == "1";
}

#define SKIP_WITHOUT_RING()                                                                   \
    do {                                                                                      \
        if (!ring_works())                                                                    \
        {                                                                                     \
            if (require_io_uring())                                                           \
            {                                                                                 \
                FAIL() << "io_uring_setup failed (" << std::strerror(errno) << ") but "        \
                       << "RUNAI_STREAMER_REQUIRE_IO_URING=1 says this host has io_uring";    \
            }                                                                                 \
            GTEST_SKIP() << "io_uring unavailable (" << std::strerror(errno)                  \
                         << "); run `make -C cpp test_iouring` where it is expected to work"; \
        }                                                                                     \
    } while (0)

AsyncIoConfig config_with(unsigned depth)
{
    AsyncIoConfig config;
    config.depth = depth;
    config.chunk_bytesize = 1 << 20;
    return config;
}

// A file of known bytes, plus the fd to read it through.
struct Fixture
{
    explicit Fixture(size_t bytesize) :
        data(utils::random::buffer(bytesize)),
        file(data),
        fd(::open(file.path.c_str(), O_RDONLY))
    {
        EXPECT_GE(fd, 0) << "open " << file.path << ": " << std::strerror(errno);
    }

    ~Fixture()
    {
        if (fd >= 0)
        {
            ::close(fd);
        }
    }

    FileRef ref() const { return FileRef{ fd, false }; }

    // What the file holds at [offset, offset + bytesize).
    std::vector<char> expected_at(size_t offset, size_t bytesize) const
    {
        return std::vector<char>(data.begin() + offset, data.begin() + offset + bytesize);
    }

    std::vector<uint8_t> data;
    utils::temp::File file;
    int fd = -1;
};

// Reap until `count` completions have arrived, or the engine reports one too many rounds of nothing.
std::vector<Completion> reap(IoUringEngine & engine, unsigned count)
{
    std::vector<Completion> out;
    std::vector<Completion> batch(count);

    for (unsigned round = 0; round < count + 8 && out.size() < count; ++round)
    {
        unsigned got = 0;
        EXPECT_EQ(engine.wait_for_completions(batch.data(), batch.size(), got, WaitMode::Block, 2000),
                  ResponseCode::Success);
        out.insert(out.end(), batch.begin(), batch.begin() + got);
    }

    EXPECT_EQ(out.size(), count) << "only " << out.size() << " of " << count << " completions arrived";
    return out;
}

} // namespace

// The ring's real size, not the requested one. io_uring rounds entries up to a power of two, and the
// caller's in-flight window is sized from depth() - so reporting the request instead would leave slots
// unused, and reporting anything LARGER than the ring would let the window exceed the queue.
TEST(IoUringEngine, Depth_Is_The_Rings_Real_Size)
{
    SKIP_WITHOUT_RING();

    const IoUringEngine exact(config_with(512));
    EXPECT_EQ(exact.depth(), 512u);

    const IoUringEngine rounded(config_with(700));
    EXPECT_EQ(rounded.depth(), 1024u) << "700 entries rounds up to 1024";
    EXPECT_GE(rounded.depth(), 700u) << "the ring must never be smaller than the window";
}

// One read, end to end: the bytes must be the file's bytes, at the right offset.
TEST(IoUringEngine, Reads_A_File)
{
    SKIP_WITHOUT_RING();

    Fixture fixture(64 << 10);
    IoUringEngine engine(config_with(8));

    std::vector<char> buffer(4096);
    ASSERT_EQ(engine.stage(7, fixture.ref(), 8192, buffer.size(), buffer.data()), ResponseCode::Success);

    unsigned issued = 0;
    ASSERT_EQ(engine.flush(issued), ResponseCode::Success);
    EXPECT_EQ(issued, 1u);

    const auto completions = reap(engine, 1);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].id, 7u) << "user_data must come back as the id we staged under";
    EXPECT_EQ(completions[0].ret, ResponseCode::Success);
    EXPECT_EQ(completions[0].bytes_transferred, buffer.size());
    EXPECT_EQ(buffer, fixture.expected_at(8192, buffer.size()));
}

// Ids are echoed, not positional. Completions arrive in whatever order the kernel finishes them, so
// routing depends entirely on user_data surviving the round trip.
TEST(IoUringEngine, Ids_Survive_The_Round_Trip)
{
    SKIP_WITHOUT_RING();

    Fixture fixture(256 << 10);
    IoUringEngine engine(config_with(16));

    // Ids well outside [0, depth) - they are opaque tokens, not slot indices, and the worker's are
    // monotonic and never reused.
    const std::vector<RequestId> ids{ 1000, 999999, 4294967296ULL };
    std::vector<std::vector<char>> buffers(ids.size(), std::vector<char>(4096));

    for (size_t i = 0; i < ids.size(); ++i)
    {
        ASSERT_EQ(engine.stage(ids[i], fixture.ref(), i * 4096, 4096, buffers[i].data()),
                  ResponseCode::Success);
    }

    unsigned issued = 0;
    ASSERT_EQ(engine.flush(issued), ResponseCode::Success);
    EXPECT_EQ(issued, ids.size());

    const auto completions = reap(engine, ids.size());

    for (const auto & completion : completions)
    {
        const auto it = std::find(ids.begin(), ids.end(), completion.id);
        ASSERT_NE(it, ids.end()) << "completion for an id never staged: " << completion.id;

        const auto index = std::distance(ids.begin(), it);
        EXPECT_EQ(completion.ret, ResponseCode::Success);
        EXPECT_EQ(buffers[index], fixture.expected_at(index * 4096, 4096))
            << "id " << completion.id << " wrote the wrong region";
    }
}

// Reading past the end returns FEWER bytes with no error. Treating "no error" as "all bytes arrived"
// is how a tensor gets silently truncated, so bytes_transferred is the value that matters.
TEST(IoUringEngine, Short_Read_At_Eof_Is_Not_An_Error)
{
    SKIP_WITHOUT_RING();

    Fixture fixture(4096);
    IoUringEngine engine(config_with(8));

    std::vector<char> buffer(8192);   // asking for twice the file
    ASSERT_EQ(engine.stage(1, fixture.ref(), 0, buffer.size(), buffer.data()), ResponseCode::Success);

    unsigned issued = 0;
    ASSERT_EQ(engine.flush(issued), ResponseCode::Success);

    const auto completions = reap(engine, 1);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions[0].ret, ResponseCode::Success) << "a short read is not a failure";
    EXPECT_EQ(completions[0].bytes_transferred, 4096u) << "and it must say how much actually arrived";
}

// A bad fd fails that one request and nothing else. The error arrives as a completion, not as a
// staging or submission failure - which is why the caller must check every cqe->res.
TEST(IoUringEngine, Bad_Fd_Fails_Only_Its_Own_Request)
{
    SKIP_WITHOUT_RING();

    Fixture fixture(16 << 10);
    IoUringEngine engine(config_with(8));

    std::vector<char> good(4096);
    std::vector<char> bad(4096);

    ASSERT_EQ(engine.stage(1, fixture.ref(), 0, good.size(), good.data()), ResponseCode::Success);
    ASSERT_EQ(engine.stage(2, FileRef{ -1, false }, 0, bad.size(), bad.data()), ResponseCode::Success)
        << "staging cannot know the fd is bad; only the completion can";

    unsigned issued = 0;
    ASSERT_EQ(engine.flush(issued), ResponseCode::Success);

    const auto completions = reap(engine, 2);
    ASSERT_EQ(completions.size(), 2u);

    for (const auto & completion : completions)
    {
        if (completion.id == 1)
        {
            EXPECT_EQ(completion.ret, ResponseCode::Success);
            EXPECT_EQ(good, fixture.expected_at(0, good.size()));
        }
        else
        {
            EXPECT_NE(completion.ret, ResponseCode::Success) << "a closed fd must not read as success";
        }
    }
}

// Staging is not issuing. flush() is the only thing that hands work to the kernel, and a worker that
// waits without having flushed waits for a completion that cannot come.
TEST(IoUringEngine, Staging_Does_Not_Issue)
{
    SKIP_WITHOUT_RING();

    Fixture fixture(16 << 10);
    IoUringEngine engine(config_with(8));

    std::vector<char> buffer(4096);
    ASSERT_EQ(engine.stage(1, fixture.ref(), 0, buffer.size(), buffer.data()), ResponseCode::Success);

    // Nothing submitted yet, so nothing can complete.
    Completion completions[4];
    unsigned count = 0;
    ASSERT_EQ(engine.wait_for_completions(completions, 4, count, WaitMode::NonBlocking),
              ResponseCode::Success);
    EXPECT_EQ(count, 0u) << "a staged request completed without ever being submitted";

    unsigned issued = 0;
    ASSERT_EQ(engine.flush(issued), ResponseCode::Success);
    EXPECT_EQ(issued, 1u);
    EXPECT_EQ(reap(engine, 1).size(), 1u);
}

// An expired wait is Success with nothing harvested, never an error - it is also the teardown
// wake-up, since no other thread may touch the engine.
TEST(IoUringEngine, Expired_Wait_Is_Not_An_Error)
{
    SKIP_WITHOUT_RING();

    IoUringEngine engine(config_with(8));   // nothing staged, nothing issued

    Completion completions[4];
    unsigned count = 0;
    EXPECT_EQ(engine.wait_for_completions(completions, 4, count, WaitMode::Block, 50),
              ResponseCode::Success);
    EXPECT_EQ(count, 0u);
}

// flush() with nothing staged is a no-op, not a syscall and not an error.
TEST(IoUringEngine, Flush_With_Nothing_Staged)
{
    SKIP_WITHOUT_RING();

    IoUringEngine engine(config_with(8));

    unsigned issued = 7;   // must be overwritten
    EXPECT_EQ(engine.flush(issued), ResponseCode::Success);
    EXPECT_EQ(issued, 0u);
}

// The whole window at once: depth requests staged, one flush, all of them complete and land where
// they belong. This is the shape a real workload takes.
TEST(IoUringEngine, Fills_The_Window)
{
    SKIP_WITHOUT_RING();

    constexpr unsigned Depth = 32;
    constexpr size_t Bytes = 4096;

    Fixture fixture(Depth * Bytes);
    IoUringEngine engine(config_with(Depth));
    ASSERT_EQ(engine.depth(), Depth);

    std::vector<std::vector<char>> buffers(Depth, std::vector<char>(Bytes));
    for (unsigned i = 0; i < Depth; ++i)
    {
        ASSERT_EQ(engine.stage(i + 1, fixture.ref(), i * Bytes, Bytes, buffers[i].data()),
                  ResponseCode::Success)
            << "staged " << i << " of " << Depth << " - the ring must hold a full window";
    }

    unsigned issued = 0;
    ASSERT_EQ(engine.flush(issued), ResponseCode::Success);
    EXPECT_EQ(issued, Depth);

    const auto completions = reap(engine, Depth);
    ASSERT_EQ(completions.size(), Depth);

    for (const auto & completion : completions)
    {
        ASSERT_EQ(completion.ret, ResponseCode::Success);
        const auto index = completion.id - 1;
        EXPECT_EQ(buffers[index], fixture.expected_at(index * Bytes, Bytes));
    }
}

}; // namespace runai::llm::streamer::common::posix_io
