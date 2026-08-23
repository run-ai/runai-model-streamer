#include "streamer/impl/pools/backend_pools.h"

#include <gtest/gtest.h>

#include <sys/sysmacros.h>   // makedev

#include <atomic>
#include <memory>

#include "common/response_code/response_code.h"
#include "utils/temp/env/env.h"

namespace runai::llm::streamer::impl
{

using Pool = BackendPools::Pool;

namespace
{
// Empty workloads (size() == 0) are a no-op in execute(), so this filesystem handler just exercises lazy
// pool creation/reuse without needing the full batch/reader machinery.
void run(Workload && workload, std::atomic<bool> & stopped)
{
    workload.execute(stopped);
}

// The object-storage pool is a per-worker pool; these tests exercise pool lifecycle, not worker behavior,
// so its factory builds a trivial no-op worker (a real ObjectStorageWorker would build a plugin client from
// the first workload's params). This lets the tests push an empty Workload{} harmlessly.
struct NoopWorker : utils::Worker<Workload>
{
    void execute(Workload &&, std::atomic<bool> &) override {}
    void drain(std::atomic<bool> &) override {}
    bool idle() const override { return true; }
};

std::unique_ptr<utils::Worker<Workload>> noop_factory()
{
    return std::make_unique<NoopWorker>();
}
} // namespace

// The default is ONE engine for everything: the throughput case for splitting is unmeasured, and
// io_uring itself is off by default for the same reason. So a streamer that reads three mounts still
// builds one engine unless someone raises the variable.
TEST(BackendPools, DefaultsToOneEngineForAllMounts)
{
    utils::temp::UnsetEnv max_engines(std::string("RUNAI_STREAMER_FS_MAX_ENGINES"));

    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    pools.push_async(makedev(8, 1), Workload{});
    pools.push_async(makedev(8, 2), Workload{});
    pools.push_async(makedev(259, 0), Workload{});

    EXPECT_EQ(pools.async_engines(), 1u) << "the default cap is 1";
}

// Raised, each mount gets its own engine - which is the isolation the split exists for.
TEST(BackendPools, EnginePerMountUpToTheCap)
{
    utils::temp::Env max_engines(std::string("RUNAI_STREAMER_FS_MAX_ENGINES"), 4UL);

    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    pools.push_async(makedev(8, 1), Workload{});
    EXPECT_EQ(pools.async_engines(), 1u);

    pools.push_async(makedev(8, 2), Workload{});
    EXPECT_EQ(pools.async_engines(), 2u);

    pools.push_async(makedev(259, 0), Workload{});
    EXPECT_EQ(pools.async_engines(), 3u);
}

// The same mount keeps the same engine however often it is pushed to - assignment is stable, because
// completion routing and window credit live with the engine.
TEST(BackendPools, SameMountReusesItsEngine)
{
    utils::temp::Env max_engines(std::string("RUNAI_STREAMER_FS_MAX_ENGINES"), 4UL);

    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    for (int i = 0; i < 5; ++i)
    {
        pools.push_async(makedev(8, 1), Workload{});
    }

    EXPECT_EQ(pools.async_engines(), 1u) << "one mount must never build a second engine";
}

// Past the cap, mounts SHARE rather than queueing for a free engine - queueing would put a second
// head-of-line problem at the assignment layer.
TEST(BackendPools, PastTheCapMountsShare)
{
    utils::temp::Env max_engines(std::string("RUNAI_STREAMER_FS_MAX_ENGINES"), 2UL);

    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    pools.push_async(makedev(8, 1), Workload{});
    pools.push_async(makedev(8, 2), Workload{});
    EXPECT_EQ(pools.async_engines(), 2u);

    // The third and fourth mounts must not create engines, and must not be refused either.
    pools.push_async(makedev(8, 3), Workload{});
    pools.push_async(makedev(8, 4), Workload{});
    EXPECT_EQ(pools.async_engines(), 2u) << "the cap must bound engines, not reject work";
}

// Engines are lazy like the synchronous pool: a streamer that never reads a mount never builds a ring
// or a thread for it.
TEST(BackendPools, AsyncEnginesAreLazy)
{
    utils::temp::Env max_engines(std::string("RUNAI_STREAMER_FS_MAX_ENGINES"), 4UL);

    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    EXPECT_EQ(pools.async_engines(), 0u);
    EXPECT_EQ(pools.pools_created(), 0u);

    pools.push_async(makedev(8, 1), Workload{});
    EXPECT_EQ(pools.async_engines(), 1u);
    EXPECT_EQ(pools.pools_created(), 1u) << "and it counts among the pools";
}

TEST(BackendPools, FilesystemPoolCreatedLazilyOnPush)
{
    BackendPools pools(run, noop_factory, noop_factory, /*filesystem_size=*/2, /*object_storage_size=*/3);

    EXPECT_EQ(pools.pools_created(), 0u);

    pools.push(Pool::FileSystem, Workload{});
    EXPECT_EQ(pools.pools_created(), 1u);

    // reusing the filesystem pool does not create another
    pools.push(Pool::FileSystem, Workload{});
    EXPECT_EQ(pools.pools_created(), 1u);
}

TEST(BackendPools, ObjectStoragePoolCreatedByPluginLock)
{
    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    EXPECT_EQ(pools.pools_created(), 0u);

    // locking the plugin builds the object-storage pool
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::S3), common::ResponseCode::Success);
    EXPECT_EQ(pools.pools_created(), 1u);

    // a repeated lock of the same plugin does not create another, and the pool now accepts workloads
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::S3), common::ResponseCode::Success);
    pools.push(Pool::ObjectStorage, Workload{});
    EXPECT_EQ(pools.pools_created(), 1u);
}

TEST(BackendPools, BothKindsCreateTwoPools)
{
    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    pools.push(Pool::FileSystem, Workload{});
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::Azure), common::ResponseCode::Success);
    EXPECT_EQ(pools.pools_created(), 2u);
}

TEST(BackendPools, ObjectPluginLockedToOne)
{
    BackendPools pools(run, noop_factory, noop_factory, 2, 3);

    // first object-storage plugin wins; the same plugin is accepted; a different one is rejected
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::GCS), common::ResponseCode::Success);
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::GCS), common::ResponseCode::Success);
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::S3), common::ResponseCode::UnsupportedBackendMix);

    // the rejected plugin did not create a second pool
    EXPECT_EQ(pools.pools_created(), 1u);
}

}; // namespace runai::llm::streamer::impl
