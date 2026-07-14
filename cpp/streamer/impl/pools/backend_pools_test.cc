#include "streamer/impl/pools/backend_pools.h"

#include <gtest/gtest.h>

#include <atomic>

#include "common/response_code/response_code.h"

namespace runai::llm::streamer::impl
{

using Kind = BackendPools::Kind;

namespace
{
// Empty workloads (size() == 0) are a no-op in execute(), so this just exercises lazy pool
// creation/reuse without needing the full batch/reader machinery.
void run(Workload && workload, std::atomic<bool> & stopped)
{
    workload.execute(stopped);
}
} // namespace

TEST(BackendPools, FilesystemPoolCreatedLazilyOnPush)
{
    BackendPools pools(run, /*filesystem_size=*/2, /*object_storage_size=*/3);

    EXPECT_EQ(pools.pools_created(), 0u);

    pools.push(Kind::FileSystem, Workload{});
    EXPECT_EQ(pools.pools_created(), 1u);

    // reusing the filesystem pool does not create another
    pools.push(Kind::FileSystem, Workload{});
    EXPECT_EQ(pools.pools_created(), 1u);
}

TEST(BackendPools, ObjectStoragePoolCreatedByPluginLock)
{
    BackendPools pools(run, 2, 3);

    EXPECT_EQ(pools.pools_created(), 0u);

    // locking the plugin builds the object-storage pool (its workers are plugin-specific)
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::S3), common::ResponseCode::Success);
    EXPECT_EQ(pools.pools_created(), 1u);

    // a repeated lock of the same plugin does not create another, and the pool now accepts workloads
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::S3), common::ResponseCode::Success);
    pools.push(Kind::ObjectStorage, Workload{});
    EXPECT_EQ(pools.pools_created(), 1u);
}

TEST(BackendPools, BothKindsCreateTwoPools)
{
    BackendPools pools(run, 2, 3);

    pools.push(Kind::FileSystem, Workload{});
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::Azure), common::ResponseCode::Success);
    EXPECT_EQ(pools.pools_created(), 2u);
}

TEST(BackendPools, ObjectPluginLockedToOne)
{
    BackendPools pools(run, 2, 3);

    // first object-storage plugin wins; the same plugin is accepted; a different one is rejected
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::GCS), common::ResponseCode::Success);
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::GCS), common::ResponseCode::Success);
    EXPECT_EQ(pools.lock_object_plugin(BackendPools::Plugin::S3), common::ResponseCode::UnsupportedBackendMix);

    // the rejected plugin did not create a second pool
    EXPECT_EQ(pools.pools_created(), 1u);
}

}; // namespace runai::llm::streamer::impl
