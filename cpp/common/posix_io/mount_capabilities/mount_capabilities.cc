#include "common/posix_io/mount_capabilities/mount_capabilities.h"

#include <linux/magic.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>   // major/minor - moved out of sys/types.h in glibc 2.28
#include <sys/vfs.h>

#include <cerrno>
#include <cstring>

#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::posix_io
{

namespace
{

// tmpfs and ramfs hold their pages in the page cache, so there is no device to bypass and O_DIRECT
// cannot work - which is what routes them to the synchronous pool.
//
// This is the ONLY probe that detects the case. An O_DIRECT open does not: measured, opening a tmpfs
// file with O_RDONLY | O_DIRECT SUCCEEDS, and the failure comes later at read time.
bool is_memory_backed(const struct statfs & fs)
{
    return fs.f_type == TMPFS_MAGIC || fs.f_type == RAMFS_MAGIC;
}

} // namespace

std::ostream & operator<<(std::ostream & os, const MountCapability & capability)
{
    return os << "mount " << major(capability.dev) << ":" << minor(capability.dev)
              << (capability.memory_backed ? " memory-backed" : "");
}

MountCapability MountCapabilities::remember(dev_t dev, bool memory_backed)
{
    const MountCapability capability{ dev, memory_backed };

    std::unique_lock<std::mutex> lock(_mutex);

    // try_emplace, not assignment: two threads can probe the same mount at once, and the results are
    // identical, so the first one wins and the second is a no-op rather than a race worth avoiding.
    const auto [it, inserted] = _mounts.try_emplace(dev, capability);
    if (inserted)
    {
        LOG(DEBUG) << "Probed " << capability;
    }
    return it->second;
}

MountCapability MountCapabilities::of_path(const std::string & path)
{
    // stat before statfs so a missing file is reported as such rather than as a mount failure.
    struct stat st;
    if (::stat(path.c_str(), &st) != 0)
    {
        LOG(ERROR) << "Failed to stat " << path << " : " << std::strerror(errno);
        throw common::Exception(common::ResponseCode::FileAccessError);
    }

    {
        // Known mount: answer from the cache and issue no statfs at all. This is the whole point - a
        // model's shards share a mount, so only the first one costs anything.
        std::unique_lock<std::mutex> lock(_mutex);
        const auto it = _mounts.find(st.st_dev);
        if (it != _mounts.end())
        {
            return it->second;
        }
    }

    struct statfs fs;
    if (::statfs(path.c_str(), &fs) != 0)
    {
        LOG(ERROR) << "Failed to statfs " << path << " : " << std::strerror(errno);
        throw common::Exception(common::ResponseCode::FileAccessError);
    }

    return remember(st.st_dev, is_memory_backed(fs));
}

MountCapability MountCapabilities::of_fd(int fd)
{
    struct stat st;
    if (::fstat(fd, &st) != 0)
    {
        LOG(ERROR) << "Failed to fstat " << fd << " : " << std::strerror(errno);
        throw common::Exception(common::ResponseCode::FileAccessError);
    }

    {
        std::unique_lock<std::mutex> lock(_mutex);
        const auto it = _mounts.find(st.st_dev);
        if (it != _mounts.end())
        {
            return it->second;
        }
    }

    struct statfs fs;
    if (::fstatfs(fd, &fs) != 0)
    {
        LOG(ERROR) << "Failed to fstatfs " << fd << " : " << std::strerror(errno);
        throw common::Exception(common::ResponseCode::FileAccessError);
    }

    return remember(st.st_dev, is_memory_backed(fs));
}

size_t MountCapabilities::size() const
{
    std::unique_lock<std::mutex> lock(_mutex);
    return _mounts.size();
}

}; // namespace runai::llm::streamer::common::posix_io
