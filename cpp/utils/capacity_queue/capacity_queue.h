#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <optional>
#include <utility>

namespace runai::llm::streamer::utils
{

// CapacityQueue - a credit-bounded pending queue.
//
// Holds items that are waiting to be submitted and bounds how much "work" may be
// in flight at once. The owner submits items by calling try_take() (which reserves
// the item's cost against the capacity) and releases credit by calling complete()
// when an item finishes, which lets the next pending item become takeable.
//
// cost and capacity are in caller-defined units: bytes for the S3 in-flight window
// (bandwidth-delay product), or an operation count for threadpool-bounded backends.
//
// NOT THREAD SAFE - by design. It is meant to be owned by a single worker that
// enqueues, takes and completes on one thread; the cross-thread completion signal
// (e.g. an async callback) is delivered through a separate thread-safe queue, and
// the worker calls complete() from its own loop. Keeping this object single-owner
// makes it a deterministic, independently testable unit.
template <typename T>
class CapacityQueue
{
 public:
    // The bucket used when a caller does not group its items. Any single value would do; what matters
    // is that all ungrouped items share it, so they form one FIFO.
    static constexpr uint64_t DefaultGroup = 0;

    // max_active_groups bounds how many groups are rotated over at once. 0 means no bound, which is
    // what an ungrouped caller wants.
    //
    // A bound is needed because more groups is not monotonically better: for the filesystem reader a
    // group is a file, and past the measured knee more files contend for the same connections and
    // throughput FALLS - 19.12 GB/s over sixteen files against 18.45 over thirty-two. Without this,
    // rotation would spread over every file that happens to be pending.
    explicit CapacityQueue(size_t capacity, size_t max_active_groups = 0) :
        _capacity(capacity),
        _max_active_groups(max_active_groups)
    {}

    // Add an item to be submitted later.
    // cost  - the amount of capacity this item consumes while in flight.
    // group - which round-robin bucket it belongs to; see try_take().
    //
    // The default group means "no grouping", and a queue whose items all use it behaves exactly as a
    // FIFO. Object storage passes nothing and is unaffected.
    void enqueue(T item, size_t cost, uint64_t group = DefaultGroup)
    {
        auto & bucket = _groups[group];
        if (bucket.empty())
        {
            admit(group);
        }
        bucket.push_back(Entry{std::move(item), cost});
        ++_pending_count;
    }

    // Return the next item to submit and reserve its cost against the capacity, or
    // std::nullopt if the window is currently full.
    //
    // When nothing is in flight, one item is always returned even if its cost
    // exceeds the whole capacity - otherwise an item larger than the window could
    // never be submitted (deadlock).
    // Selection point: round-robin over groups, FIFO within a group.
    //
    // Callers that pass no group get one bucket, so the rotation has nothing to rotate over and this
    // is a plain FIFO - which is what object storage relies on.
    //
    // The async filesystem reader groups by FILE. Taking every chunk of one file before the next
    // leaves an NFS mount mostly idle, because one file's read stream uses one of the client's
    // connections; rotating spreads the outstanding reads over several files and fills the link.
    // Measured: 11.34 GB/s reading one file at a time against 19.12 across sixteen.
    //
    // Rotation is preserved ACROSS calls, so chunks belonging to different workloads interleave too -
    // which is the point, since one workload usually holds only two or three files.
    std::optional<T> try_take()
    {
        if (_active.empty())
        {
            return std::nullopt;
        }

        const uint64_t group = _active.front();
        auto it = _groups.find(group);

        // Cost is read before the capacity test so an over-large item is still admitted when nothing
        // is in flight - otherwise it could never be submitted at all.
        const size_t cost = it->second.front().cost;

        if (_inflight != 0 && _inflight + cost > _capacity)
        {
            return std::nullopt;
        }

        Entry entry = std::move(it->second.front());
        it->second.pop_front();
        --_pending_count;

        // Move this group to the back so the next take starts at a different file. A group that is
        // now empty leaves the active set, and a waiting one takes its place - which is what keeps
        // the number of files being read from at once at the configured width rather than at however
        // many happen to be pending.
        _active.pop_front();
        if (it->second.empty())
        {
            _groups.erase(it);
            promote();
        }
        else
        {
            _active.push_back(group);
        }

        _inflight += entry.cost;
        return std::optional<T>(std::move(entry.item));
    }

    // Release the credit previously reserved for a completed item. Pass the same
    // cost that try_take() reserved for it. Clamps at zero defensively.
    void complete(size_t cost)
    {
        _inflight = (_inflight >= cost) ? _inflight - cost : 0;
    }

    // Drop every pending item and release all reserved credit, leaving the queue idle()
    // in one step. For aborting a window whose pending set may exceed the capacity, where
    // draining via try_take()/complete() would stop at the full-window boundary. The
    // owner must abandon any tracking tied to the dropped items - their costs are gone.
    void clear()
    {
        _groups.clear();
        _active.clear();
        _waiting.clear();
        _pending_count = 0;
        _inflight = 0;
    }

    // no items left to submit (some may still be in flight)
    bool empty() const
    {
        return _pending_count == 0;
    }

    // nothing left to submit and nothing in flight
    bool idle() const
    {
        return _pending_count == 0 && _inflight == 0;
    }

    size_t inflight() const
    {
        return _inflight;
    }

    size_t pending() const
    {
        return _pending_count;
    }

    // How many distinct groups still have something to submit, whether or not they are being rotated
    // over yet.
    size_t groups() const
    {
        return _groups.size();
    }

    // How many groups are being rotated over right now. For the filesystem reader this is how many
    // FILES are read from at once, which is the number the throughput depends on.
    size_t active_groups() const
    {
        return _active.size();
    }

    size_t capacity() const
    {
        return _capacity;
    }

 private:
    struct Entry
    {
        T item;
        size_t cost;
    };

    // A group with work joins the rotation if there is room, and queues otherwise.
    void admit(uint64_t group)
    {
        if (_max_active_groups == 0 || _active.size() < _max_active_groups)
        {
            _active.push_back(group);
        }
        else
        {
            _waiting.push_back(group);
        }
    }

    // Fill the place a finished group left. Skips anything that has since been emptied and re-added,
    // which cannot happen today but would silently give one group two turns if it ever did.
    void promote()
    {
        while (!_waiting.empty())
        {
            const uint64_t next = _waiting.front();
            _waiting.pop_front();
            if (_groups.count(next) != 0)
            {
                _active.push_back(next);
                return;
            }
        }
    }

    size_t _capacity;
    size_t _inflight = 0;

    size_t _max_active_groups;

    // FIFO within a group, round-robin between the ACTIVE groups; see the selection point in
    // try_take().
    //
    // A group appears in _active or _waiting, never both, and EXACTLY ONCE - enqueue only admits when
    // a bucket was empty, and try_take re-appends only when it is not yet empty. Anything that let a
    // group appear twice would give it two turns per rotation and quietly weight it double.
    std::map<uint64_t, std::deque<Entry>> _groups;
    std::deque<uint64_t> _active;
    std::deque<uint64_t> _waiting;

    // Kept rather than summed over the buckets: empty() and idle() are called on every loop pass,
    // and walking a map there would be a cost for nothing.
    size_t _pending_count = 0;
};

} // namespace runai::llm::streamer::utils
