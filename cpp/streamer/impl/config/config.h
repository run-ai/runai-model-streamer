
#pragma once

#include <string>

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
//     RUNAI_STREAMER_FS_QUEUE_DEPTH     -> fs_async_queue_depth (512). File system only, and
//                                          NODE-WIDE - divided per process, see AsyncIoSettings.
//     RUNAI_STREAMER_FS_STRATEGY        -> fs_strategy_candidates ("sync_buffered"). An ordered
//                                          preference list; the first the host can serve wins.

struct Config
{
    // fs_async_chunk_bytesize comes last, after the bool, only so existing callers that pass
    // enforce_minimum positionally keep working.
    Config(unsigned concurrency,
           unsigned s3_concurrency,
           size_t s3_block_bytesize,
           size_t fs_sync_read_block_bytesize,
           bool enforce_minimum = true,
           size_t fs_async_chunk_bytesize = default_fs_async_chunk_bytesize,
           unsigned fs_async_queue_depth = default_fs_async_queue_depth,
           std::string fs_strategy_candidates = default_fs_strategy_candidates);
    Config(bool enforce_minimum = true);

    unsigned max_concurrency() const;

    static constexpr size_t min_fs_sync_read_block_bytesize = 2 * 1024 * 1024;

    // No shared floor with the synchronous block size: 2 MiB suits a reader that wants fewer, larger
    // reads, while an async reader with depth wants more, smaller ones.
    static constexpr size_t default_fs_async_chunk_bytesize = 8 * 1024 * 1024;

    // Node-wide, so it means the same thing at TP=1 and TP=8. 512 matches what InstantTensor uses
    // before its own division by world size.
    static constexpr unsigned default_fs_async_queue_depth = 512;

    // The synchronous reader, until a measurement says otherwise. io_uring is reachable only by
    // asking for it: S5's exit criterion is an A/B against the threadpool, and defaulting to the new
    // path before that would decide by omission what the measurement is meant to decide.
    static constexpr const char * default_fs_strategy_candidates = "sync_buffered";

    unsigned concurrency;
    unsigned s3_concurrency;
    size_t s3_block_bytesize;
    size_t fs_sync_read_block_bytesize;

    // Also where tasks are cut: a task never crosses a chunk boundary, so a completed chunk always
    // covers a whole number of tasks. Cut on both paths, to keep one rule rather than two.
    size_t fs_async_chunk_bytesize;

    // An ordered preference list, best first, parsed by parse_candidates. Only the DEFAULT - the
    // caller may override it until the first submission resolves it (StrategyResolver).
    std::string fs_strategy_candidates;

    // In-flight requests for the whole node. What one process may hold is this divided by the number
    // of streamer processes on the node, which is not known this early - AsyncIoSettings does it.
    unsigned fs_async_queue_depth;
};

std::ostream & operator<<(std::ostream &, const Config &);

}; // namespace runai::llm::streamer::impl
