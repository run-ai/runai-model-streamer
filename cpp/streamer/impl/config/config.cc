#include "streamer/impl/config/config.h"

#include <algorithm>

#include "common/s3_wrapper/s3_wrapper.h"

#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

Config::Config(unsigned concurrency, unsigned s3_concurrency, size_t s3_block_bytesize, size_t fs_sync_read_block_bytesize,
               bool enforce_minimum, size_t fs_async_chunk_bytesize, unsigned fs_async_queue_depth) :
    concurrency(concurrency),
    s3_concurrency(s3_concurrency),
    s3_block_bytesize(s3_block_bytesize),
    fs_sync_read_block_bytesize(fs_sync_read_block_bytesize),
    fs_async_chunk_bytesize(fs_async_chunk_bytesize),
    fs_async_queue_depth(fs_async_queue_depth)
{
    ASSERT(concurrency) << " threadpool size must be a positive number";
    ASSERT(s3_block_bytesize) << "s3 chunk bytesize must be positive";

    // Tasks are cut on multiples of this, so zero would divide by zero rather than merely misbehave.
    ASSERT(fs_async_chunk_bytesize) << "file system chunk bytesize must be positive";
    ASSERT(fs_async_queue_depth) << "file system queue depth must be positive";

    if (enforce_minimum)
    {
        if (s3_block_bytesize < common::s3::S3ClientWrapper::min_chunk_bytesize)
        {
            // For S3 requests the chunk size is the minimal value of 5 MB
            LOG(INFO) << "Setting s3 reading block size to 5 MiB";
            this->s3_block_bytesize = common::s3::S3ClientWrapper::min_chunk_bytesize;
        }

        if (fs_sync_read_block_bytesize < min_fs_sync_read_block_bytesize)
        {
            LOG(INFO) << "Setting file system reading block size to 2 MiB";
            this->fs_sync_read_block_bytesize = min_fs_sync_read_block_bytesize;
        }
    }
}

Config::Config(bool enforce_minimum /* = true */) :
    Config(utils::getenv<unsigned long>("RUNAI_STREAMER_CONCURRENCY", 16UL),
           utils::getenv<unsigned long>("RUNAI_STREAMER_CONCURRENCY", 8UL),
           utils::getenv<size_t>("RUNAI_STREAMER_CHUNK_BYTESIZE", common::s3::S3ClientWrapper::default_chunk_bytesize),
           utils::getenv<size_t>("RUNAI_STREAMER_CHUNK_BYTESIZE", min_fs_sync_read_block_bytesize),
           enforce_minimum,
           utils::getenv<size_t>("RUNAI_STREAMER_FS_CHUNK_BYTESIZE", default_fs_async_chunk_bytesize),
           utils::getenv<unsigned long>("RUNAI_STREAMER_FS_QUEUE_DEPTH", default_fs_async_queue_depth))
{}

unsigned Config::max_concurrency() const
{
    return std::max(concurrency, s3_concurrency);
}

std::ostream & operator<<(std::ostream & os, const Config & config)
{
    return os << "Streamer concurrency " << config.concurrency << " ; s3 concurrency " << config.s3_concurrency << " ; s3 block size " << config.s3_block_bytesize << " bytes; " << " ; file system block size " << config.fs_sync_read_block_bytesize << " bytes; " << " ; file system chunk size " << config.fs_async_chunk_bytesize << " bytes; " << " ; file system queue depth " << config.fs_async_queue_depth << " (node-wide); ";
}

}; // namespace runai::llm::streamer::impl
