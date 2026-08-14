
#pragma once

#include <chrono>
#include <ostream>

namespace runai::llm::streamer::impl
{

// Two environment variables configure both backends, each with its own default when unset (config.cc):
//     RUNAI_STREAMER_CONCURRENCY     -> concurrency (file system, default 16) and s3_concurrency (object
//                                      storage, default 8). One variable, two defaults: setting it
//                                      overrides BOTH backends with the same value.
//     RUNAI_STREAMER_CHUNK_BYTESIZE  -> fs_block_bytesize and s3_block_bytesize, likewise.

// Reading from file system path
//     concurrency :       number of readers - default 16
//     fs_block_bytesize : number of bytes in a single os call to read from file - minimum and default is 2 MiB

// Reading from S3 path
//     s3_concurrency :    number of asynchronous S3 clients - default 8
//     s3_block_bytesize : number of bytes in a single request to the S3 client - minimum is 5 MiB and default is 8 MiB

struct Config
{
    Config(unsigned concurrency,
           unsigned s3_concurrency,
           size_t s3_block_bytesize,
           size_t fs_block_bytesize,
           bool enforce_minimum = true,
           unsigned long object_storage_retry_timeout_seconds = 0);
    Config(bool enforce_minimum = true);

    unsigned max_concurrency() const;

    static constexpr size_t min_fs_block_bytesize = 2 * 1024 * 1024;

    unsigned concurrency;
    unsigned s3_concurrency;
    size_t s3_block_bytesize;
    size_t fs_block_bytesize;
    // Application-level retry budget for each object chunk, starting when that chunk is first submitted to
    // the backend. Zero preserves fail-fast behavior after the storage plugin's native retry policy expires.
    std::chrono::seconds object_storage_retry_timeout;
};

std::ostream & operator<<(std::ostream &, const Config &);

}; // namespace runai::llm::streamer::impl
