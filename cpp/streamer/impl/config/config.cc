#include "streamer/impl/config/config.h"

#include "posix_io/alignment/alignment.h"

#include <utility>

#include <algorithm>

#include "common/s3_wrapper/s3_wrapper.h"

#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

Config::Config(unsigned concurrency, unsigned s3_concurrency, size_t s3_block_bytesize, size_t fs_sync_read_block_bytesize,
               bool enforce_minimum, size_t fs_async_chunk_bytesize, unsigned fs_async_queue_depth,
               std::string fs_strategy_candidates, unsigned long object_storage_retry_timeout_seconds) :
    concurrency(concurrency),
    s3_concurrency(s3_concurrency),
    s3_block_bytesize(s3_block_bytesize),
    fs_sync_read_block_bytesize(fs_sync_read_block_bytesize),
    fs_async_chunk_bytesize(fs_async_chunk_bytesize),
    fs_strategy_candidates(std::move(fs_strategy_candidates)),
    fs_async_queue_depth(fs_async_queue_depth),
    object_storage_retry_timeout(object_storage_retry_timeout_seconds)
{
    // Resolved here, with the other configuration, so a malformed RUNAI_STREAMER_DIRECT_BLOCK fails
    // like every other malformed variable: runai_start builds a Config first and turns any failure
    // into InvalidParameterError, while a failure in the Streamer that follows becomes UnknownError.
    //
    // It has to be forced, because the value lives behind a cached static and is otherwise resolved
    // lazily on the first read - so a typo would surface mid-load, as a failed read, with nothing
    // naming the variable that caused it.
    (void)posix_io::direct_block_size();

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
           utils::getenv<unsigned long>("RUNAI_STREAMER_FS_QUEUE_DEPTH", default_fs_async_queue_depth),
           utils::getenv<std::string>("RUNAI_STREAMER_FS_STRATEGY", default_fs_strategy_candidates),
           utils::getenv<unsigned long>("RUNAI_STREAMER_S3_TIMEOUT", 0UL))
{}

unsigned Config::max_concurrency() const
{
    return std::max(concurrency, s3_concurrency);
}

std::ostream & operator<<(std::ostream & os, const Config & config)
{
    return os << "Streamer concurrency " << config.concurrency << " ; s3 concurrency " << config.s3_concurrency << " ; s3 block size " << config.s3_block_bytesize << " bytes; " << " ; file system block size " << config.fs_sync_read_block_bytesize << " bytes; " << " ; file system chunk size " << config.fs_async_chunk_bytesize << " bytes; " << " ; file system queue depth " << config.fs_async_queue_depth << " (node-wide); " << " ; file system strategy " << config.fs_strategy_candidates << "; object storage retry timeout " << config.object_storage_retry_timeout.count() << " seconds; ";
}

}; // namespace runai::llm::streamer::impl
