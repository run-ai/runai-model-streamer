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
// No alignment here, on purpose. Alignment is PAGE_SIZE by construction: O_DIRECT needs the logical
// block size (512 or 4096) and a page is a multiple of both, so page alignment is always sufficient
// and can only be stricter than needed. Chunk sizes are megabytes, so it costs nothing.
//
// Whether the mount can serve O_DIRECT is not here either, but it is answerable - see
// MountCapabilities::direct_support(). It is kept out of this struct because this one is filled by a
// stat and a statfs on a path, while that question needs a file that can be opened. Two different
// inputs, so two calls.
struct MountCapability
{
    dev_t dev = 0;
    bool  memory_backed = false;   // tmpfs / ramfs
};

// Whether a mount can serve O_DIRECT.
//
// Unknown is a real answer and has to be handled. The probe opens a file, so it can fail for reasons
// that say nothing about the mount: the file is missing, or we may not read it. Reporting those as
// "no" would send a whole mount to the synchronous reader because of one bad path.
enum class DirectSupport { Unknown, Yes, No };

std::ostream & operator<<(std::ostream &, DirectSupport);

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

    // Can this mount serve O_DIRECT? Probed once per mount by opening `file_path` with
    // O_RDONLY | O_DIRECT and closing it again, then remembered by `dev`.
    //
    // The open is the whole test. Measured: a filesystem without O_DIRECT refuses it with EINVAL, and
    // one with O_DIRECT accepts it. So the answer comes from the mount itself rather than from a guess
    // about filesystem types.
    //
    // STATX_DIOALIGN would be the official way to ask, and it is unusable twice over. It arrived in
    // kernel 6.1 while our floor is 5.15. It is also missing from the build headers here even on a 6.8
    // kernel - struct statx ends at stx_mnt_id, with no stx_dio_mem_align field to read - so using it
    // would mean declaring our own struct. The trial open works on every kernel, and it keeps one
    // mechanism answering this question instead of two that could disagree.
    //
    // `file_path` must name a file. A directory can never be opened with O_DIRECT, whatever the mount
    // supports - measured, EINVAL - so probing with one would report every mount as incapable.
    //
    // Only libaio needs this. libaio is asynchronous only with O_DIRECT. Without it, io_submit reads
    // inline and one thread serves one file at a time, which is worse than the 16-thread pool it would
    // replace (design 5.7). A mount without O_DIRECT therefore has to reach the synchronous reader
    // before anything is dispatched, because the worker cannot re-route a workload it already holds.
    // io_uring needs none of this, since a buffered read on the ring is still asynchronous.
    //
    // This does not report the block size. If a mount's real alignment were 512, congruence would be
    // eight times easier to satisfy than at 4096 and more files could be read directly. Only
    // STATX_DIOALIGN can say, so that stays future work.
    DirectSupport direct_support(dev_t dev, const std::string & file_path);

    // How many distinct mounts have been seen. For tests and for logging how much the cache saved.
    size_t size() const;

 private:
    // Takes the answer, not the struct: naming statfs here would declare it inside this namespace
    // as an incomplete type, and the system one would never be found.
    MountCapability remember(dev_t dev, bool memory_backed);

    // Records a settled O_DIRECT answer and returns it, so callers read as one line.
    DirectSupport remember_direct(dev_t dev, bool supported);

    mutable std::mutex _mutex;
    std::map<dev_t, MountCapability> _mounts;

    // Separate from _mounts, and holding only settled answers.
    //
    // Separate because the two are learned from different things - _mounts from a stat of any path,
    // this from opening a file - so a mount is normally in _mounts before this can be asked. Folding
    // them together would mean caching a MountCapability with an unanswered field in it, which reads
    // as "no" to everyone who forgets to check.
    //
    // Only settled answers: an Unknown probe writes nothing, so the next file on that mount asks
    // again. That is what stops one missing path from condemning a whole mount.
    std::map<dev_t, bool> _direct;
};

}; // namespace runai::llm::streamer::common::posix_io
