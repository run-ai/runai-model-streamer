#include "common/posix_io/engine_factory/engine_factory.h"

#include <gtest/gtest.h>

#include <sys/syscall.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <string>

namespace runai::llm::streamer::common::posix_io
{

namespace
{

#ifndef __NR_io_uring_setup
#define __NR_io_uring_setup 425
#endif

// The kernel, directly - not IoUringProbe and not the factory. An expectation computed by the thing
// it checks is no expectation at all.
bool ring_works()
{
    struct io_uring_params_stub { char opaque[512]; } params;
    std::memset(&params, 0, sizeof(params));

    const int fd = ::syscall(__NR_io_uring_setup, 8, &params);
    if (fd < 0)
    {
        return false;
    }

    ::close(fd);
    return true;
}

bool require_io_uring()
{
    const char * value = std::getenv("RUNAI_STREAMER_REQUIRE_IO_URING");
    return value != nullptr && std::string(value) == "1";
}

AsyncIoConfig config()
{
    AsyncIoConfig out;
    out.depth = 32;
    out.chunk_bytesize = 1 << 20;
    return out;
}

} // namespace

// The whole point of the factory: nullptr means "not available here", and it must track what the
// kernel will actually do rather than what the strategy asked for.
TEST(EngineFactory, IoUringBuffered_Follows_The_Kernel)
{
    auto engine = make_io_engine(Strategy::IoUringBuffered, config());

    if (ring_works())
    {
        ASSERT_NE(engine, nullptr) << "io_uring works here but the factory refused to build it";
        EXPECT_GE(engine->depth(), 32u) << "the window is sized from this; it may round up, never down";
    }
    else
    {
        if (require_io_uring())
        {
            FAIL() << "RUNAI_STREAMER_REQUIRE_IO_URING=1 but io_uring_setup failed ("
                   << std::strerror(errno) << ")";
        }
        EXPECT_EQ(engine, nullptr) << "io_uring is blocked here; building an engine cannot succeed";
    }
}

// StrategyResolver reports both io_uring strategies as available wherever the ring is. This test
// checks that the factory agrees with it.
//
// If the factory refused one of them, resolution would still succeed and the first workload would
// then fail because it has no engine. That failure would be reported as a routing bug, which would
// send anyone debugging it to the wrong place.
//
// Both can be built because they use the same ring. O_DIRECT is a property of the file, and the
// worker opens each file itself.
TEST(EngineFactory, Both_IoUring_Strategies_Build_Or_Refuse_Together)
{
    auto direct = make_io_engine(Strategy::IoUringDirect, config());
    auto buffered = make_io_engine(Strategy::IoUringBuffered, config());

    EXPECT_EQ(direct == nullptr, buffered == nullptr)
        << "the resolver offers these two together, so the factory must serve them together";

    if (ring_works())
    {
        ASSERT_NE(direct, nullptr) << "io_uring works here but the factory refused the direct strategy";

        // The caller sizes its in-flight window from depth(). Both strategies build the same ring, so
        // both must report the same depth - otherwise the window would depend on which strategy was
        // resolved.
        EXPECT_EQ(direct->depth(), buffered->depth());
    }
}

// libaio has no engine yet, so the factory returns nullptr and the dispatcher tries the next
// candidate. This is different from a broken host: nothing was attempted. Remove this test in S8.
TEST(EngineFactory, Libaio_Is_Not_Implemented_Yet)
{
    EXPECT_EQ(make_io_engine(Strategy::LibaioDirect, config()), nullptr);
}

// SyncBuffered is the alternative to an engine, not one of them. Asking for it is a routing bug, and
// silently returning nullptr would let it look like an availability problem.
TEST(EngineFactory, Synchronous_Strategy_Is_Rejected)
{
    EXPECT_THROW(make_io_engine(Strategy::SyncBuffered, config()), std::exception);
}

}; // namespace runai::llm::streamer::common::posix_io
