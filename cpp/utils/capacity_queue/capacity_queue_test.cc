#include "utils/capacity_queue/capacity_queue.h"

#include <gtest/gtest.h>
#include <set>

#include <optional>
#include <vector>

namespace runai::llm::streamer::utils
{

TEST(CapacityQueue, EmptyIsIdleAndTakesNothing)
{
    CapacityQueue<int> q(100);

    EXPECT_TRUE(q.empty());
    EXPECT_TRUE(q.idle());
    EXPECT_EQ(q.pending(), 0U);
    EXPECT_EQ(q.inflight(), 0U);
    EXPECT_FALSE(q.try_take().has_value());
}

TEST(CapacityQueue, TakesUntilWindowFull)
{
    CapacityQueue<int> q(100);
    q.enqueue(1, 40);
    q.enqueue(2, 40);
    q.enqueue(3, 40);

    EXPECT_EQ(q.pending(), 3U);

    auto a = q.try_take();          // inflight 0 -> 40
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(*a, 1);
    EXPECT_EQ(q.inflight(), 40U);

    auto b = q.try_take();          // 40 + 40 = 80 <= 100
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(*b, 2);
    EXPECT_EQ(q.inflight(), 80U);

    auto c = q.try_take();          // 80 + 40 = 120 > 100 -> blocked
    EXPECT_FALSE(c.has_value());
    EXPECT_EQ(q.inflight(), 80U);
    EXPECT_EQ(q.pending(), 1U);
}

TEST(CapacityQueue, CompleteFreesCredit)
{
    CapacityQueue<int> q(100);
    q.enqueue(1, 40);
    q.enqueue(2, 40);
    q.enqueue(3, 40);

    ASSERT_TRUE(q.try_take().has_value());   // inflight 40
    ASSERT_TRUE(q.try_take().has_value());   // inflight 80
    ASSERT_FALSE(q.try_take().has_value());  // full

    q.complete(40);                          // inflight 40
    EXPECT_EQ(q.inflight(), 40U);

    auto c = q.try_take();                   // 40 + 40 = 80 <= 100
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(*c, 3);
    EXPECT_EQ(q.inflight(), 80U);
}

TEST(CapacityQueue, AlwaysTakesOneWhenIdleEvenIfOverCapacity)
{
    CapacityQueue<int> q(10);
    q.enqueue(1, 1000);   // larger than the whole window
    q.enqueue(2, 5);

    auto a = q.try_take();   // inflight 0 -> allowed despite cost > capacity
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(*a, 1);
    EXPECT_EQ(q.inflight(), 1000U);

    // now something is in flight, so the next item must wait
    EXPECT_FALSE(q.try_take().has_value());

    q.complete(1000);
    auto b = q.try_take();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(*b, 2);
}

TEST(CapacityQueue, PreservesFifoOrder)
{
    CapacityQueue<int> q(1000);
    for (int i = 0; i < 5; ++i)
    {
        q.enqueue(i, 1);
    }
    for (int i = 0; i < 5; ++i)
    {
        auto v = q.try_take();
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, i);
    }
    EXPECT_TRUE(q.empty());
}

TEST(CapacityQueue, CompleteClampsAtZero)
{
    CapacityQueue<int> q(100);
    q.enqueue(1, 30);
    ASSERT_TRUE(q.try_take().has_value());   // inflight 30
    q.complete(1000);                        // over-complete
    EXPECT_EQ(q.inflight(), 0U);
    EXPECT_TRUE(q.idle());
}

TEST(CapacityQueue, CompletionDrivenDrainNeverExceedsCapacity)
{
    constexpr size_t capacity = 100;
    constexpr size_t cost = 30;
    constexpr int total = 10;

    CapacityQueue<int> q(capacity);
    for (int i = 0; i < total; ++i)
    {
        q.enqueue(i, cost);
    }

    std::vector<int> taken;
    int completed = 0;

    auto pump = [&]
    {
        while (auto v = q.try_take())
        {
            taken.push_back(*v);
            EXPECT_LE(q.inflight(), capacity);   // costs are <= capacity, so never exceeded
        }
    };

    pump();
    while (completed < total)
    {
        ASSERT_GT(q.inflight(), 0U);   // something must be in flight to complete
        q.complete(cost);
        ++completed;
        pump();
    }

    // everything drained, in FIFO order, and the queue is idle
    ASSERT_EQ(static_cast<int>(taken.size()), total);
    for (int i = 0; i < total; ++i)
    {
        EXPECT_EQ(taken[i], i);
    }
    EXPECT_TRUE(q.idle());
}

TEST(CapacityQueue, ClearDropsPendingAndInflightInOneStep)
{
    // pending far exceeds the window, and some items are in flight: a try_take()/complete() drain would stop
    // at the full-window boundary, but clear() empties both in one step and leaves the queue idle().
    constexpr size_t capacity = 10;
    CapacityQueue<int> q(capacity);
    for (int i = 0; i < 100; ++i)   // 100 chunks, cost 1 -> pending >> capacity
    {
        q.enqueue(i, 1);
    }

    for (int i = 0; i < static_cast<int>(capacity); ++i)   // fill the window
    {
        ASSERT_TRUE(q.try_take().has_value());
    }
    ASSERT_FALSE(q.try_take().has_value());   // full: try_take() blocks with items still pending
    EXPECT_EQ(q.inflight(), capacity);
    EXPECT_EQ(q.pending(), 90U);

    q.clear();

    EXPECT_TRUE(q.idle());
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(q.pending(), 0U);
    EXPECT_EQ(q.inflight(), 0U);
    EXPECT_FALSE(q.try_take().has_value());

    // usable after clear(): still bounded by the same capacity
    q.enqueue(7, 1);
    auto v = q.try_take();
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(*v, 7);
    EXPECT_EQ(q.capacity(), capacity);
}


// ---- grouping -----------------------------------------------------------------------------------
//
// Groups exist for the filesystem reader, where a group is a FILE. Reading one file to the end before
// starting the next leaves an NFS mount mostly idle, because one file's read stream uses one of the
// client's connections - measured at 11.34 GB/s against 19.12 across sixteen files.

// Object storage passes no group at all, and MUST keep the FIFO it had before groups existed. This is
// the compatibility guarantee for every caller that does not group.
TEST(CapacityQueue, UngroupedItemsStayFifo)
{
    CapacityQueue<int> q(100);

    for (int i = 0; i < 5; ++i)
    {
        q.enqueue(i, 1);
    }

    for (int i = 0; i < 5; ++i)
    {
        const auto item = q.try_take();
        ASSERT_TRUE(item.has_value());
        EXPECT_EQ(*item, i) << "ungrouped items must come back in the order they went in";
    }
}

// Round-robin BETWEEN groups, FIFO WITHIN one. Taking every item of a group before the next is what
// this exists to prevent.
TEST(CapacityQueue, RotatesBetweenGroups)
{
    CapacityQueue<int> q(100);

    // Two groups, three items each, all of group 10 enqueued first.
    for (int i = 0; i < 3; ++i)
    {
        q.enqueue(100 + i, 1, 10);
    }
    for (int i = 0; i < 3; ++i)
    {
        q.enqueue(200 + i, 1, 20);
    }

    // Despite the enqueue order, the takes alternate.
    const std::vector<int> expected{ 100, 200, 101, 201, 102, 202 };
    for (const auto want : expected)
    {
        const auto item = q.try_take();
        ASSERT_TRUE(item.has_value());
        EXPECT_EQ(*item, want) << "expected the rotation to alternate groups";
    }
}

// The width is a budget, not "all of them". Past the measured knee more files contend for the same
// connections and throughput FALLS - 19.12 GB/s over sixteen files against 18.45 over thirty-two - so
// an unbounded rotation would spread over every group that happens to be pending.
TEST(CapacityQueue, RotatesOverAtMostTheConfiguredGroups)
{
    CapacityQueue<int> q(100, 2 /* max active groups */);

    for (int g = 0; g < 4; ++g)
    {
        for (int i = 0; i < 2; ++i)
        {
            q.enqueue(g * 10 + i, 1, static_cast<uint64_t>(g));
        }
    }

    EXPECT_EQ(q.groups(), 4u) << "all four have work";
    EXPECT_EQ(q.active_groups(), 2u) << "only two may be rotated over";

    // The first four takes touch groups 0 and 1 only.
    std::set<int> seen;
    for (int i = 0; i < 4; ++i)
    {
        const auto item = q.try_take();
        ASSERT_TRUE(item.has_value());
        seen.insert(*item / 10);
    }
    EXPECT_EQ(seen, (std::set<int>{ 0, 1 })) << "a third group must not be started early";
}

// A group that empties leaves the rotation and a waiting one takes its place - which is what keeps
// the number of files being read from at the configured width rather than decaying towards one.
TEST(CapacityQueue, AWaitingGroupIsPromotedWhenOneFinishes)
{
    CapacityQueue<int> q(100, 2);

    q.enqueue(1, 1, 10);
    q.enqueue(2, 1, 20);
    q.enqueue(3, 1, 30);   // waits: the active set is full

    ASSERT_EQ(q.active_groups(), 2u);

    // Drain group 10 entirely.
    ASSERT_TRUE(q.try_take().has_value());   // from 10
    ASSERT_TRUE(q.try_take().has_value());   // from 20

    // 10 is now empty, so 30 should have joined.
    EXPECT_EQ(q.active_groups(), 1u) << "10 left, and 30 is promoted on the next take";

    const auto last = q.try_take();
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(*last, 3) << "the waiting group must be reached, not stranded";
    EXPECT_TRUE(q.idle() || q.empty());
}

// pending() counts items, not groups, and empty()/idle() must agree with it. These are called on
// every loop pass, so a wrong answer here stalls the worker rather than merely misreporting.
TEST(CapacityQueue, CountsAndEmptinessAreRightAcrossGroups)
{
    CapacityQueue<int> q(100, 2);

    q.enqueue(1, 1, 10);
    q.enqueue(2, 1, 20);
    q.enqueue(3, 1, 30);

    EXPECT_EQ(q.pending(), 3u);
    EXPECT_FALSE(q.empty());
    EXPECT_FALSE(q.idle());

    for (int i = 0; i < 3; ++i)
    {
        ASSERT_TRUE(q.try_take().has_value());
    }

    EXPECT_EQ(q.pending(), 0u);
    EXPECT_TRUE(q.empty());
    EXPECT_FALSE(q.idle()) << "three are in flight, so it is empty but not idle";

    for (int i = 0; i < 3; ++i)
    {
        q.complete(1);
    }
    EXPECT_TRUE(q.idle());
}

// clear() has to drop the group bookkeeping too. Leaving a group listed with no items would make
// try_take dereference an erased bucket.
TEST(CapacityQueue, ClearDropsGroupsAsWell)
{
    CapacityQueue<int> q(100, 2);

    q.enqueue(1, 1, 10);
    q.enqueue(2, 1, 20);
    q.enqueue(3, 1, 30);
    ASSERT_TRUE(q.try_take().has_value());

    q.clear();

    EXPECT_EQ(q.groups(), 0u);
    EXPECT_EQ(q.active_groups(), 0u);
    EXPECT_EQ(q.pending(), 0u);
    EXPECT_TRUE(q.idle());
    EXPECT_FALSE(q.try_take().has_value()) << "taking from a cleared queue must not touch stale state";
}

} // namespace runai::llm::streamer::utils
