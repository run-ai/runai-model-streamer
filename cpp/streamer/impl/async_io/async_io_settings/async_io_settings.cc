#include "streamer/impl/async_io/async_io_settings/async_io_settings.h"

#include <algorithm>

#include "common/posix_io/io_engine/io_engine.h"
#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

namespace
{

// The NODE-WIDE file budget, before it is divided by the process group.
//
// Read here rather than through Config, like RUNAI_STREAMER_PROCESS_GROUP_SIZE beside it, because
// both are properties of how this node is being shared rather than of a request.
unsigned files_budget()
{
    return std::max(1UL, utils::getenv<unsigned long>("RUNAI_STREAMER_FS_FILES_IN_FLIGHT",
                                                      static_cast<unsigned long>(AsyncIoSettings::DefaultFilesBudget)));
}

} // namespace

AsyncIoSettings::AsyncIoSettings(const Config & config, size_t max_read_bytesize) :
    _process_group_size(std::max(1UL, utils::getenv<unsigned long>("RUNAI_STREAMER_PROCESS_GROUP_SIZE", 1UL))),
    _depth(std::min(std::max(config.fs_async_queue_depth / _process_group_size, MinDepth), MaxDepth)),
    _files_in_flight(std::min(std::max(files_budget() / _process_group_size, MinFiles), MaxFiles)),
    _chunk_bytesize(std::min(config.fs_async_chunk_bytesize, max_read_bytesize))
{
    const auto divided = config.fs_async_queue_depth / _process_group_size;

    // Said whenever the share is not the budget, because reading one file at a time is what this
    // exists to prevent and it is invisible otherwise - a slow read looks like slow storage.
    const auto files_divided = files_budget() / _process_group_size;

    if (_process_group_size > 1)
    {
        LOG(INFO) << "File budget " << files_budget() << " over " << _process_group_size
                  << " processes on this node gives " << _files_in_flight << " files per process";
    }

    if (files_divided < MinFiles)
    {
        LOG(WARNING) << "File budget resolves to " << files_divided << " per process ("
                     << files_budget() << " over " << _process_group_size << "); raised to "
                     << MinFiles << ", below which this process reads one file at a time and cannot"
                     << " fill a mount that spreads work over several connections";
    }
    else if (files_divided > MaxFiles)
    {
        LOG(WARNING) << "File budget " << files_divided << " per process exceeds the " << MaxFiles
                     << " cap; clamped. Past the measured knee more files contend for the same"
                     << " connections and throughput falls";
    }

    if (_process_group_size > 1)
    {
        LOG(INFO) << "Queue depth " << config.fs_async_queue_depth << " over " << _process_group_size
                  << " processes on this node gives " << _depth << " per process";
    }

    // Logged rather than applied silently: a configured number that never materialises is worse than
    // one that is rejected.
    if (divided < MinDepth)
    {
        LOG(WARNING) << "Queue depth resolves to " << divided << " per process ("
                     << config.fs_async_queue_depth << " over " << _process_group_size
                     << "); raised to " << MinDepth << ", below which reads are effectively serial";
    }
    else if (divided > MaxDepth)
    {
        LOG(WARNING) << "Queue depth " << divided << " per process exceeds the " << MaxDepth
                     << " cap; clamped. Depth beyond this cannot add throughput - bytes in flight is"
                     << " what saturates a device, and this is already far past it";
    }

    if (_chunk_bytesize != config.fs_async_chunk_bytesize)
    {
        LOG(WARNING) << "Chunk size " << config.fs_async_chunk_bytesize << " exceeds the kernel's "
                     << max_read_bytesize << " per read; clamped";
    }
}

AsyncIoSettings::AsyncIoSettings(const Config & config) :
    AsyncIoSettings(config, common::posix_io::max_read_bytesize())
{}

unsigned AsyncIoSettings::depth() const
{
    return _depth;
}

size_t AsyncIoSettings::chunk_bytesize() const
{
    return _chunk_bytesize;
}

unsigned AsyncIoSettings::files_in_flight() const
{
    return _files_in_flight;
}

unsigned AsyncIoSettings::process_group_size() const
{
    return _process_group_size;
}

std::ostream & operator<<(std::ostream & os, const AsyncIoSettings & settings)
{
    return os << "async io: depth " << settings.depth()
              << " (of " << settings.process_group_size() << " processes on this node)"
              << ", chunk " << settings.chunk_bytesize() << " bytes"
              // The product, not just the factors: bytes in flight is what saturates a device, and it
              // is the only way a small chunk with a large depth reads as what it is.
              << ", up to " << (static_cast<size_t>(settings.depth()) * settings.chunk_bytesize())
              << " bytes in flight"
              // In the same line as depth because the two are read together: depth says how much is
              // outstanding, this says over how many files, and only the pair explains a rate.
              << ", across " << settings.files_in_flight() << " files at once";
}

}; // namespace runai::llm::streamer::impl
