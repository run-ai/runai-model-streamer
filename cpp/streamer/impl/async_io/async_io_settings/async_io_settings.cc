#include "streamer/impl/async_io/async_io_settings/async_io_settings.h"

#include <algorithm>

#include "common/posix_io/io_engine/io_engine.h"
#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

AsyncIoSettings::AsyncIoSettings(const Config & config, size_t max_read_bytesize) :
    _process_group_size(std::max(1UL, utils::getenv<unsigned long>("RUNAI_STREAMER_PROCESS_GROUP_SIZE", 1UL))),
    _depth(std::max(1U, config.fs_async_queue_depth / _process_group_size)),
    _chunk_bytesize(std::min(config.fs_async_chunk_bytesize, max_read_bytesize))
{
    if (_process_group_size > 1)
    {
        LOG(INFO) << "Queue depth " << config.fs_async_queue_depth << " over " << _process_group_size
                  << " processes on this node gives " << _depth << " per process";
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

unsigned AsyncIoSettings::process_group_size() const
{
    return _process_group_size;
}

std::ostream & operator<<(std::ostream & os, const AsyncIoSettings & settings)
{
    return os << "async io: depth " << settings.depth()
              << " (of " << settings.process_group_size() << " processes on this node)"
              << ", chunk " << settings.chunk_bytesize() << " bytes";
}

}; // namespace runai::llm::streamer::impl
