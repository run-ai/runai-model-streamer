#include "streamer/impl/async_io/inflight_chunks/inflight_chunks.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

common::posix_io::RequestId InflightChunks::add(const Chunk & chunk)
{
    const auto id = _next_id++;

    // 32 bits at ~1,280 requests/s wraps after about a month of continuous streaming, and a streamer
    // does not outlive a model load. If it ever could, a wrapped id could collide with a live one -
    // which is the aliasing this class exists to avoid - so it is asserted rather than assumed.
    ASSERT(_chunks.count(id) == 0) << "request id " << id << " wrapped onto a live request";

    _chunks[id] = InflightChunk{ chunk, chunk.offset, chunk.bytesize };
    return id;
}

const InflightChunk * InflightChunks::find(common::posix_io::RequestId id) const
{
    const auto it = _chunks.find(id);
    return it == _chunks.end() ? nullptr : &it->second;
}

Progress InflightChunks::record(common::posix_io::RequestId id, size_t bytes_transferred)
{
    const auto it = _chunks.find(id);
    ASSERT(it != _chunks.end()) << "no in-flight chunk for request " << id << " - check find() first";

    auto & entry = it->second;

    // Compared against what THIS pass asked for, never against the original extent. After a short
    // read those differ, and using the extent would judge every later pass short as well.
    if (bytes_transferred >= entry.remaining)
    {
        entry.cursor += entry.remaining;
        entry.remaining = 0;
        return Progress::Complete;
    }

    if (bytes_transferred == 0)
    {
        // Not an error from the kernel's point of view - it simply had nothing more to give. The
        // caller asked for bytes the file does not have, which is EofError, not success.
        LOG(ERROR) << "Read returned no further bytes at offset " << entry.cursor
                   << " with " << entry.remaining << " still owed";
        return Progress::Eof;
    }

    entry.cursor += bytes_transferred;
    entry.remaining -= bytes_transferred;
    return Progress::Partial;
}

Chunk InflightChunks::pending(common::posix_io::RequestId id) const
{
    const auto it = _chunks.find(id);
    ASSERT(it != _chunks.end()) << "no in-flight chunk for request " << id;

    const auto & entry = it->second;

    // The destination advances with the cursor: the bytes already read went to the front of the
    // buffer, so the rest goes after them.
    Chunk rest = entry.chunk;
    rest.offset = entry.cursor;
    rest.bytesize = entry.remaining;
    rest.buffer = entry.chunk.buffer + (entry.cursor - entry.chunk.offset);
    return rest;
}

Chunk InflightChunks::release(common::posix_io::RequestId id)
{
    const auto it = _chunks.find(id);
    ASSERT(it != _chunks.end()) << "no in-flight chunk for request " << id;

    const Chunk chunk = it->second.chunk;
    _chunks.erase(it);
    return chunk;
}

size_t InflightChunks::size() const
{
    return _chunks.size();
}

}; // namespace runai::llm::streamer::impl
