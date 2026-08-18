#pragma once

#include <cstddef>
#include <map>

#include "common/posix_io/io_engine/io_engine.h"
#include "streamer/impl/async_io/chunk_splitter/chunk_splitter.h"

namespace runai::llm::streamer::impl
{

// What one staged chunk is worth remembering while it is in flight.
//
// THREE lengths, not one. "The requested bytesize" is ambiguous once a short read can be re-issued,
// and comparing a completion against the wrong one mis-accounts the second pass with no error
// anywhere:
//
//   chunk.bytesize  the original extent - what the tasks are accounted against once ALL of it lands
//   cursor          where the next re-stage resumes
//   remaining       what THIS pass's completion is compared against
//
// Ask for 8 MiB, get 5: cursor advances by 5 and remaining becomes 3. With a single field the next
// completion of 3 would be compared against 8, judged short again, and re-issued forever.
struct InflightChunk
{
    Chunk  chunk;
    size_t cursor = 0;
    size_t remaining = 0;
};

// What a completion meant for the chunk it belongs to.
enum class Progress
{
    Complete,   // the whole extent has landed - its tasks can be accounted
    Partial,    // some bytes arrived; re-stage the rest, on the same entry
    Eof,        // zero further bytes while bytes were still owed - the file is shorter than asked
};

// Chunks issued and not yet finished, by request id.
//
// Ids are MONOTONIC AND NEVER REUSED, deliberately - not indices into a table sized `depth`. Reuse
// would let a completion that arrives after its request was abandoned land on whatever now holds that
// id, routing it to the wrong batch and the wrong destination with no error. Never reusing makes such
// a completion simply absent from the table, which is how it is recognised and dropped. Object
// storage does the same, for the same reason (object_storage_worker.cc:325-328).
//
// The in-flight bound is NOT here - CapacityQueue already enforces it. This is only a way to find a
// request's context again, which is why a map costs nothing worth counting: ~128 us/s at 1,280
// requests/s, against 5.4 rejecting fixed files for saving 64 us/s as unmeasurable.
//
// NOT THREAD SAFE. One worker owns it, like everything else on this path.
class InflightChunks
{
 public:
    // Record a chunk about to be staged, and return the id to stage it under.
    common::posix_io::RequestId add(const Chunk & chunk);

    // The entry for `id`, or nullptr if there is none - which is exactly how a late completion from an
    // abandoned request is recognised. Callers must check rather than assume.
    const InflightChunk * find(common::posix_io::RequestId id) const;

    // Account one completion of `bytes_transferred` against `id`.
    //
    // Complete and Eof leave the entry in place; the caller then reads it and calls release(). Partial
    // advances cursor and remaining, so the entry is ready to be re-staged with pending() below.
    //
    // ASSERTs if `id` is not live: only the caller can know whether an unknown id is a stale
    // completion to drop or its own bookkeeping gone wrong, so it must check find() first.
    Progress record(common::posix_io::RequestId id, size_t bytes_transferred);

    // What still has to be read for `id`: where to resume, how much is left, and where it goes. Only
    // meaningful after a Partial.
    Chunk pending(common::posix_io::RequestId id) const;

    // Remove `id` and return the chunk it was reading, so the caller can account its tasks.
    Chunk release(common::posix_io::RequestId id);

    size_t size() const;

 private:
    // Starts at 1 so that 0 is never a live id, leaving it usable as an unmistakable "not a real
    // request" value - the same reason object storage starts its counter at 1.
    common::posix_io::RequestId _next_id = 1;

    std::map<common::posix_io::RequestId, InflightChunk> _chunks;
};

}; // namespace runai::llm::streamer::impl
