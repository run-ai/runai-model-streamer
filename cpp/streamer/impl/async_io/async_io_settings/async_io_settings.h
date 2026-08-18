#pragma once

#include <cstddef>
#include <ostream>

#include "streamer/impl/config/config.h"

namespace runai::llm::streamer::impl
{

// What this process will actually use, as opposed to what the operator asked for.
//
// CONSTRUCT ON THE FIRST WORKLOAD, never at streamer construction. Depth is divided by
// RUNAI_STREAMER_PROCESS_GROUP_SIZE, which the Python layer does not write until stream_files() -
// long after runai_start() has returned. Building this early reads the unset default of 1, skips the
// division, and says nothing: the device then sees eight times the intended depth at TP=8.
//
// Every value is clamped against its real ceiling here and logged when clamped. A configured number
// that quietly never materialises is worse than one that is rejected.
class AsyncIoSettings
{
 public:
    // max_read_bytesize defaults to this host's kernel ceiling; it is a parameter so a test can check
    // the clamp without depending on the page size of the machine it runs on.
    explicit AsyncIoSettings(const Config & config, size_t max_read_bytesize);
    explicit AsyncIoSettings(const Config & config);

    // In-flight requests for THIS process: the node-wide figure divided by the number of streamer
    // processes on the node. Never zero - a depth of zero admits nothing at all.
    unsigned depth() const;

    // Bytes per request, clamped to what the kernel will read in one go. Beyond that the kernel
    // short-reads and the caller re-stages, so the bytes actually in flight would be depth x the cap
    // rather than depth x this - a window smaller than the one reserved against the memory limit,
    // and than the one a sweep measured.
    size_t chunk_bytesize() const;

    // Streamer processes on this node, from RUNAI_STREAMER_PROCESS_GROUP_SIZE. 1 when unset, which is
    // both the single-process case and the "Python has not written it yet" case - the second is a bug
    // and the reason this object is built late.
    unsigned process_group_size() const;

 private:
    unsigned _process_group_size;
    unsigned _depth;
    size_t _chunk_bytesize;
};

std::ostream & operator<<(std::ostream &, const AsyncIoSettings &);

}; // namespace runai::llm::streamer::impl
