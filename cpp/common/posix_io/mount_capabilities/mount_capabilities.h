#pragma once

#include <sys/types.h>

#include <map>
#include <mutex>
#include <ostream>
#include <string>

namespace runai::llm::streamer::common::posix_io
{

// What a mount can do.
//
// `dev` is st_dev, which identifies the filesystem instance. It is both the routing key and the key
// an engine is assigned by, so those two decisions read one value.
//
// No alignment and no "supports O_DIRECT", on purpose:
//
//   - Alignment is PAGE_SIZE by construction. O_DIRECT needs the logical block size (512 or 4096) and
//     a page is a multiple of both, so page alignment is always sufficient and can only be stricter
//     than needed. Chunk sizes are megabytes, so it costs nothing.
//   - Whether a mount can serve O_DIRECT is not determinable below kernel 6.1: STATX_DIOALIGN landed
//     there, and our floor is 5.15. `!memory_backed` would be a guess - virtiofs is FUSE-based and
//     unverified - and a wrong guess means every read falls back per file while this says it should
//     not. Better absent than wrong; the direct path adds it when it needs it.
struct MountCapability
{
    dev_t dev = 0;
    bool  memory_backed = false;   // tmpfs / ramfs
};

std::ostream & operator<<(std::ostream &, const MountCapability &);

// Probed once per mount, then remembered.
//
// A model's shards share a mount, so this turns 200 probes into one. Entries are small values and
// mounts are few, so there is no eviction, no refcounting and no bound - nothing here ever needs
// reclaiming.
//
// Thread safe. The map is written at most once per mount and read thereafter; the syscalls run
// outside the lock, and a MountCapability is returned by copy so no entry's lifetime escapes it.
class MountCapabilities
{
 public:
    // For a path not yet opened - called once per submission, on its first file. This is what the
    // dispatcher routes on and what picks the engine, so it must be answerable before batches are
    // built.
    //
    // Throws FileAccessError if the path cannot be stat'ed.
    MountCapability of_path(const std::string & path);

    // For an fd the caller was opening anyway, so it costs an fstat and no extra round trip.
    //
    // VERIFICATION, not resolution: a file need not be on its directory's mount - a symlink
    // elsewhere, a bind mount over one file - and this is what catches it. On a mismatch the caller
    // falls back for that file rather than re-routing its batches.
    MountCapability of_fd(int fd);

    // How many distinct mounts have been seen. For tests and for logging how much the cache saved.
    size_t size() const;

 private:
    // Takes the answer, not the struct: naming statfs here would declare it inside this namespace
    // as an incomplete type, and the system one would never be found.
    MountCapability remember(dev_t dev, bool memory_backed);

    mutable std::mutex _mutex;
    std::map<dev_t, MountCapability> _mounts;
};

}; // namespace runai::llm::streamer::common::posix_io
