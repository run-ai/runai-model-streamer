
#pragma once

#include <ostream>

namespace runai::llm::streamer::impl
{

// Environment variables, with their defaults when unset (config.cc):
//
//     RUNAI_STREAMER_CONCURRENCY        -> concurrency (file system, 16) AND s3_concurrency (object
//                                          storage, 8). One variable, two defaults: setting it
//                                          overrides both backends with the same value.
//     RUNAI_STREAMER_CHUNK_BYTESIZE     -> fs_sync_read_block_bytesize (2 MiB, also the minimum) AND
//                                          s3_block_bytesize (8 MiB, minimum 5 MiB), likewise.
//     RUNAI_STREAMER_FS_CHUNK_BYTESIZE  -> fs_async_chunk_bytesize (8 MiB). File system only.

struct Config
{
    // fs_async_chunk_bytesize comes last, after the bool, only so existing callers that pass
    // enforce_minimum positionally keep working.
    Config(unsigned concurrency,
           unsigned s3_concurrency,
           size_t s3_block_bytesize,
           size_t fs_sync_read_block_bytesize,
           bool enforce_minimum = true,
           size_t fs_async_chunk_bytesize = default_fs_async_chunk_bytesize);
    Config(bool enforce_minimum = true);

    unsigned max_concurrency() const;

    static constexpr size_t min_fs_sync_read_block_bytesize = 2 * 1024 * 1024;

    // No shared floor with the synchronous block size: 2 MiB suits a reader that wants fewer, larger
    // reads, while an async reader with depth wants more, smaller ones.
    static constexpr size_t default_fs_async_chunk_bytesize = 8 * 1024 * 1024;

    unsigned concurrency;
    unsigned s3_concurrency;
    size_t s3_block_bytesize;
    size_t fs_sync_read_block_bytesize;

    // Also where tasks are cut: a task never crosses a chunk boundary, so a completed chunk always
    // covers a whole number of tasks. Cut on both paths, to keep one rule rather than two.
    size_t fs_async_chunk_bytesize;
};

std::ostream & operator<<(std::ostream &, const Config &);

}; // namespace runai::llm::streamer::impl
