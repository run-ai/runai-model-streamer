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
    // processes on the node, then bounded at both ends by MinDepth and MaxDepth below.
    unsigned depth() const;

    // How many FILES this process reads from at once, divided by the process group the same way
    // depth is - because the thing being shared is node-wide, not per process.
    //
    // On an NFS mount a single file's read stream does not fill the link: the client spreads work
    // over `nconnect` TCP connections, and reads on one file use one of them. Reading file by file
    // therefore leaves most of the mount idle no matter how deep the queue is.
    //
    // MEASURED on NFS (nconnect=16), fio, libaio, 8 MiB, one reader:
    //
    //     files    1      2      4      8     16     32
    //     GB/s   11.34  13.93  16.58  18.33  19.12  18.45
    //
    // It peaks at 16 - the nconnect figure - and falls past it, where streams contend for
    // connections rather than adding any. With eight readers the same total holds: two files each is
    // flat against sixteen each, because 8 x 2 is again 16.
    //
    // Worth nothing on a filesystem without this property: virtiofs measured no file-count
    // sensitivity at all. So this is a default that helps one mount and costs nothing on the other,
    // NOT a number derived from the storage.
    unsigned files_in_flight() const;

    // Bounds on the RESOLVED depth - what becomes the ring - not on the configured node-wide value.
    //
    // The floor exists because depth 1 is a serial reader paying for the whole asynchronous
    // apparatus, and the division can produce it silently. It bites only above ~170 processes on one
    // node, where the node-wide overshoot is a few requests, so it costs nothing in practice.
    //
    // The ceiling is a SAFETY bound, not a target. What saturates a device is bytes in flight, not
    // requests: at the default 8 MiB chunk even 32 requests is ~268 MB outstanding, past bandwidth x
    // latency for any storage we run on. 1024 is roughly 34x that, and twice the largest depth
    // InstantTensor can produce - so it can only ever catch a mis-set variable, never limit
    // throughput.
    //
    // Both match InstantTensor's max(512 // world_size, 3), which is our formula and our default
    // arrived at independently, with 3 as its floor.
    static constexpr unsigned MinDepth = 3;
    static constexpr unsigned MaxDepth = 1024;

    // The node-wide file budget, and the bounds on this process's share of it.
    //
    // 16 because that is where the measured curve stops rising, and because it is this mount's
    // `nconnect`. ONE MOUNT IS NOT A LAW: a different NFS server may saturate elsewhere, so this is
    // an overridable default and not a constant to trust. RUNAI_STREAMER_FS_FILES_IN_FLIGHT sets the
    // node-wide budget.
    //
    // The floor is 2, not 1. At sixteen processes the division reaches 1, which is reading one file
    // at a time - today's behaviour and the slowest point on the curve. A large process group must
    // not silently arrive back there.
    //
    // The ceiling is a safety bound. Past 16 the measured throughput FALLS, so a mis-set variable
    // should not be able to make things worse than the default does.
    static constexpr unsigned DefaultFilesBudget = 16;
    static constexpr unsigned MinFiles = 2;
    static constexpr unsigned MaxFiles = 64;

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
    unsigned _files_in_flight;
    size_t _chunk_bytesize;
};

std::ostream & operator<<(std::ostream &, const AsyncIoSettings &);

}; // namespace runai::llm::streamer::impl
