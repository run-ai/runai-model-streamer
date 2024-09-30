#include "streamer/impl/config/config.h"

#include "common/s3_wrapper/s3_wrapper.h"

#include "utils/env/env.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

Config::Config(unsigned concurrency, size_t s3_block_bytesize, size_t fs_block_bytesize, bool enforce_minimum) :
    concurrency(concurrency),
    s3_block_bytesize(s3_block_bytesize),
    fs_block_bytesize(fs_block_bytesize)
{
    ASSERT(concurrency) << " threadpool size must be a positive number";
    ASSERT(s3_block_bytesize) << "s3 chunk bytesize must be positive";

    if (enforce_minimum)
    {
        if (s3_block_bytesize < common::s3::S3ClientWrapper::min_chunk_bytesize)
        {
            // For S3 requests the chunk size is the minimal value of 5 MB
            LOG(INFO) << "Setting s3 reading block size to 5 MiB";
            this->s3_block_bytesize = common::s3::S3ClientWrapper::min_chunk_bytesize;
        }

        if (fs_block_bytesize < min_fs_block_bytesize)
        {
            LOG(INFO) << "Setting file system reading block size to 2 MiB";
            this->fs_block_bytesize = min_fs_block_bytesize;
        }
    }
}

Config::Config(bool enforce_minimum /* = true */) :
    Config(utils::getenv<unsigned long>("RUNAI_STREAMER_CONCURRENCY", 20UL),
           utils::getenv<size_t>("RUNAI_STREAMER_CHUNK_BYTESIZE", common::s3::S3ClientWrapper::default_chunk_bytesize),
           utils::getenv<size_t>("RUNAI_STREAMER_CHUNK_BYTESIZE", min_fs_block_bytesize), enforce_minimum)
{}

std::ostream & operator<<(std::ostream & os, const Config & config)
{
    return os << "Streamer concurrency " << config.concurrency << " ; s3 block size " << config.s3_block_bytesize << " bytes; " << " ; file system block size " << config.fs_block_bytesize << " bytes; ";
}

}; // namespace runai::llm::streamer::impl
