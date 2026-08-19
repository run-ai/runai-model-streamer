#include "streamer/impl/async_io/async_io_worker/async_io_worker.h"

#include <gtest/gtest.h>

#include <dirent.h>
#include <limits.h>
#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <vector>

#include "common/posix_io/mock/mock_io_engine.h"
#include "streamer/impl/assigner/assigner.h"
#include "streamer/impl/batches/batches.h"
#include "common/exception/exception.h"

#include "utils/random/random.h"
#include "utils/temp/env/env.h"
#include "utils/temp/file/file.h"

namespace runai::llm::streamer::impl
{

namespace
{

using common::posix_io::MockIoEngine;
using common::posix_io::Strategy;
using common::posix_io::WaitMode;

constexpr size_t ChunkSize = 4096;

// One file's ranges, tiled contiguously into one buffer - so they coalesce to a single transfer, which
// is the layout a model read produces.
struct Fixture
{
    explicit Fixture(const std::vector<size_t> & range_sizes, unsigned concurrency = 1) :
        concurrency(std::string("RUNAI_STREAMER_CONCURRENCY"), static_cast<unsigned long>(concurrency)),
        chunk_bytesize(std::string("RUNAI_STREAMER_FS_CHUNK_BYTESIZE"), static_cast<unsigned long>(ChunkSize)),
        depth(std::string("RUNAI_STREAMER_FS_QUEUE_DEPTH"), 1024UL),
        group(std::string("RUNAI_STREAMER_PROCESS_GROUP_SIZE"), 1UL),
        sizes(range_sizes),
        total(std::accumulate(range_sizes.begin(), range_sizes.end(), static_cast<size_t>(0))),
        config(std::make_shared<Config>(false /* do not force minimum */)),
        responder(std::make_shared<common::Responder>(0)),
        data(utils::random::buffer(total == 0 ? 1 : total)),
        file(data),
        buffer(total == 0 ? 1 : total)
    {
        responder->increment(sizes.size());

        FileRanges ranges;
        ranges.path = file.path;
        size_t offset = 0;
        char * dst = buffer.data();
        for (const auto size : sizes)
        {
            ranges.ranges.push_back(ReadRange{ offset, size, dst });
            offset += size;
            dst += size;
        }
        request.push_back(ranges);
    }

    // One workload holding every batch, which is what a single async worker gets.
    Workload workload()
    {
        Assigner assigner(request, config);
        Workload out;
        for (const auto & transfer : assigner.transfers())
        {
            Batches batches(1 /* submission */, transfer.file_index, transfer.tasks, config, responder,
                            file.path, params, transfer.range_sizes, transfer.first_range_index);
            for (size_t i = 0; i < batches.size(); ++i)
            {
                out.add_batch(std::move(batches[i]));
            }
        }
        return out;
    }

    // What a fully-read range should contain: the mock fills from the absolute file offset.
    std::vector<char> expected_at(size_t offset, size_t bytesize) const
    {
        std::vector<char> out(bytesize);
        MockIoEngine::fill_expected(out.data(), offset, bytesize);
        return out;
    }

    std::vector<char> got_at(size_t offset, size_t bytesize) const
    {
        return std::vector<char>(buffer.begin() + offset, buffer.begin() + offset + bytesize);
    }

    utils::temp::Env concurrency;
    utils::temp::Env chunk_bytesize;
    utils::temp::Env depth;
    utils::temp::Env group;

    std::vector<size_t> sizes;
    size_t total;
    std::shared_ptr<Config> config;
    std::shared_ptr<common::Responder> responder;
    std::vector<uint8_t> data;
    utils::temp::File file;
    std::vector<char> buffer;
    std::vector<FileRanges> request;
    common::s3::S3ClientWrapper::Params params;   // invalid -> file system
};

// Drives a worker with a MockIoEngine the test controls.
struct Driver
{
    Driver() :
        worker(Strategy::IoUringBuffered,
               [this](Strategy, const common::posix_io::AsyncIoConfig & config)
               {
                   auto owned = std::make_unique<MockIoEngine>(config.depth);
                   engine = owned.get();
                   return owned;
               })
    {}

    // execute() stages everything, since the window is larger than the chunk count. Staged is not
    // issued: the worker flushes in drain(), and it counts what IT issued - so a test must never call
    // flush() itself, or the worker sees completions for reads it does not believe are outstanding.
    void execute(Workload && w) { worker.execute(std::move(w), stopped); }

    // Issues the staged reads. Nothing is ready yet, so this harvests nothing.
    void issue() { worker.drain(stopped); }

    // Harvests and routes whatever the test completed.
    void route() { worker.drain(stopped); }

    std::vector<common::posix_io::RequestId> in_flight() { return engine->in_flight(); }

    std::atomic<bool> stopped{ false };
    AsyncIoWorker worker;
    MockIoEngine * engine = nullptr;   // owned by the worker
};

// Every response, in arrival order.
//
// Bounded, never pop(): the failure this suite is most likely to catch is a range that is never
// answered, and an unbounded pop turns that into a hung test rather than a failed one.
std::vector<common::Response> drain_responses(common::Responder & responder, unsigned count)
{
    std::vector<common::Response> out;
    for (unsigned i = 0; i < count; ++i)
    {
        auto response = responder.pop(5000 /* ms */);
        EXPECT_NE(response.ret, common::ResponseCode::TimedOut)
            << "only " << i << " of " << count << " ranges were answered";
        out.push_back(response);
    }
    return out;
}

// How many descriptors this process holds open on `path`. The fd lifetime is not observable from the
// worker's interface, and it is exactly what a leak would break, so it is read from /proc.
unsigned open_fds_on(const std::string & path)
{
    // The temp file's path is relative ("./name"); /proc reports the absolute one.
    char resolved[PATH_MAX];
    EXPECT_NE(::realpath(path.c_str(), resolved), nullptr);
    const std::string absolute = resolved;

    unsigned count = 0;
    DIR * dir = ::opendir("/proc/self/fd");
    EXPECT_NE(dir, nullptr);
    if (dir == nullptr)
    {
        return 0;
    }

    while (const struct dirent * entry = ::readdir(dir))
    {
        char link[PATH_MAX];
        const std::string full = std::string("/proc/self/fd/") + entry->d_name;
        const auto len = ::readlink(full.c_str(), link, sizeof(link) - 1);
        if (len > 0)
        {
            link[len] = '\0';
            if (absolute == link)
            {
                ++count;
            }
        }
    }

    ::closedir(dir);
    return count;
}

} // namespace

// A file smaller than one chunk: one read, one response per range, and the bytes land where they
// belong.
TEST(AsyncIoWorker, Reads_And_Answers_Every_Range)
{
    Fixture fixture({ 100, 200, 300 });
    Driver driver;

    driver.execute(fixture.workload());

    ASSERT_NE(driver.engine, nullptr);
    ASSERT_EQ(driver.engine->staged_count(), 1u) << "three small ranges pack into one chunk";

    driver.issue();
    driver.engine->complete_all();
    driver.route();

    const auto responses = drain_responses(*fixture.responder, 3);

    std::set<unsigned> seen;
    for (const auto & r : responses)
    {
        EXPECT_EQ(r.ret, common::ResponseCode::Success);
        EXPECT_TRUE(seen.insert(r.index).second) << "range " << r.index << " answered twice";
    }
    EXPECT_EQ(seen, (std::set<unsigned>{ 0, 1, 2 }));

    // The data, not just the codes: a chunk staged against the wrong destination completes just as
    // cleanly. Offsets are checked where there is more than one chunk to get wrong.
    EXPECT_EQ(fixture.got_at(0, 600), fixture.expected_at(0, 600));
}

// The reason MockIoEngine exists: a later chunk landing first must not let a range be answered before
// its own bytes have arrived.
TEST(AsyncIoWorker, Out_Of_Order_Completion)
{
    Fixture fixture({ ChunkSize, ChunkSize, ChunkSize });   // one chunk each
    Driver driver;

    driver.execute(fixture.workload());
    ASSERT_EQ(driver.engine->staged_count(), 3u);

    driver.issue();

    const auto in_flight = driver.in_flight();
    ASSERT_EQ(in_flight.size(), 3u);

    // Last, first, middle.
    driver.engine->complete(in_flight[2]);
    driver.route();
    driver.engine->complete(in_flight[0]);
    driver.engine->complete(in_flight[1]);
    driver.route();

    const auto responses = drain_responses(*fixture.responder, 3);

    std::set<unsigned> seen;
    for (const auto & r : responses)
    {
        EXPECT_EQ(r.ret, common::ResponseCode::Success);
        seen.insert(r.index);
    }
    EXPECT_EQ(seen, (std::set<unsigned>{ 0, 1, 2 }));

    for (unsigned i = 0; i < 3; ++i)
    {
        EXPECT_EQ(fixture.got_at(i * ChunkSize, ChunkSize), fixture.expected_at(i * ChunkSize, ChunkSize))
            << "chunk " << i << " landed in the wrong place";
    }
}

// A short read is re-staged on the SAME request, resuming where it stopped - not reported as success,
// and not re-queued.
TEST(AsyncIoWorker, Short_Read_Is_Restaged)
{
    Fixture fixture({ ChunkSize });
    Driver driver;

    driver.execute(fixture.workload());

    driver.issue();
    const auto id = driver.in_flight().front();

    driver.engine->complete_short(id, 1000);
    driver.route();

    // Nothing answered yet - the range is not complete. A short read must never be reported as
    // success, which is the whole reason bytes_transferred is checked.
    EXPECT_EQ(fixture.responder->pop(10 /* ms */).ret, common::ResponseCode::TimedOut);

    // The remainder was staged again, on the same id, resumed at the right offset.
    ASSERT_EQ(driver.engine->staged_count(), 1u);
    const auto & restaged = driver.engine->request(id);
    EXPECT_EQ(restaged.offset, 1000u);
    EXPECT_EQ(restaged.bytesize, ChunkSize - 1000);

    driver.issue();
    driver.engine->complete(id);
    driver.route();

    const auto responses = drain_responses(*fixture.responder, 1);
    EXPECT_EQ(responses.front().ret, common::ResponseCode::Success);
    EXPECT_EQ(fixture.got_at(0, ChunkSize), fixture.expected_at(0, ChunkSize));
}

// Zero further bytes while bytes are still owed: the file is shorter than the caller asked for.
TEST(AsyncIoWorker, Eof_Is_Reported)
{
    Fixture fixture({ ChunkSize });
    Driver driver;

    driver.execute(fixture.workload());

    driver.issue();
    const auto id = driver.in_flight().front();

    driver.engine->complete_short(id, 0);
    driver.route();

    const auto responses = drain_responses(*fixture.responder, 1);
    EXPECT_EQ(responses.front().ret, common::ResponseCode::EofError);
}

// A zero-sized range reads nothing but owes a response like any other - answered at enqueue, since no
// completion will ever arrive for it.
TEST(AsyncIoWorker, Zero_Sized_Range_Is_Answered)
{
    Fixture fixture({ 100, 0, 100 });
    Driver driver;

    driver.execute(fixture.workload());

    // Answered before any I/O completes.
    const auto responses = drain_responses(*fixture.responder, 1);
    EXPECT_EQ(responses.front().index, 1);
    EXPECT_EQ(responses.front().ret, common::ResponseCode::Success);

    driver.issue();
    driver.engine->complete_all();
    driver.route();

    const auto rest = drain_responses(*fixture.responder, 2);
    for (const auto & r : rest)
    {
        EXPECT_EQ(r.ret, common::ResponseCode::Success);
    }
}

// Stopping mid-flight still owes a response for every range. Staged-but-unissued chunks are the trap:
// they hold window credit and no completion will ever arrive for them, so a teardown that walks only
// the in-flight set leaves the caller waiting forever.
TEST(AsyncIoWorker, Stop_Answers_Every_Range)
{
    Fixture fixture({ ChunkSize, ChunkSize, ChunkSize });
    Driver driver;

    driver.execute(fixture.workload());
    ASSERT_EQ(driver.engine->staged_count(), 3u) << "staged, and deliberately never issued";

    driver.stopped = true;
    driver.route();

    const auto responses = drain_responses(*fixture.responder, 3);
    for (const auto & r : responses)
    {
        EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
    }
}

// One descriptor per batch however many chunks it has, released when the workload ends.
TEST(AsyncIoWorker, Fd_Opened_Once_And_Closed_At_Finalize)
{
    Fixture fixture({ ChunkSize, ChunkSize, ChunkSize });   // three chunks, one file
    Driver driver;

    const auto before = open_fds_on(fixture.file.path);

    driver.execute(fixture.workload());
    EXPECT_EQ(open_fds_on(fixture.file.path), before + 1) << "three chunks must share one descriptor";

    driver.issue();
    driver.engine->complete_all();
    driver.route();

    drain_responses(*fixture.responder, 3);
    EXPECT_EQ(open_fds_on(fixture.file.path), before) << "descriptor leaked past the last response";
}

// Waiting with nothing issued waits on an empty ring for a completion that will never arrive. Staged
// is not issued, so the count that decides this is the issued one.
TEST(AsyncIoWorker, Does_Not_Block_With_Nothing_Issued)
{
    Fixture fixture({ ChunkSize });
    Driver driver;

    driver.execute(fixture.workload());

    // Staged but never flushed - so nothing is in flight.
    driver.engine->set_flush_stalled(true);
    driver.issue();

    ASSERT_GT(driver.engine->waits(), 0u);
    EXPECT_EQ(driver.engine->last_wait_mode(), WaitMode::NonBlocking)
        << "blocking here waits for a completion that cannot come";

    // Once something is issued, blocking is correct - and bounded.
    driver.engine->set_flush_stalled(false);
    driver.issue();
    EXPECT_EQ(driver.engine->last_wait_mode(), WaitMode::Block);
    EXPECT_GT(driver.engine->last_wait_timeout_ms(), 0u) << "an unbounded wait has no way to notice stopped";
}

}; // namespace runai::llm::streamer::impl
