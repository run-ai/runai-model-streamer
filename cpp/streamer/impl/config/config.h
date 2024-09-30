
#pragma once

#include <ostream>

namespace runai::llm::streamer::impl
{

// Reading from file system path
//     Concurrency :       number of readers - default 20
//     fs_block_bytesize : number of bytes in a single os call to read from file - minimum and default is 2 MiB

// Reading from S3 path
//     Concurrency :       number of asynchronous S3 clients - default 20
//     s3_block_bytesize : number of bytes in a single request to the S3 client - minimum is 5 MiB and default is 8 MiB

struct Config
{
    Config(unsigned concurrency, size_t s3_block_bytesize, size_t fs_block_bytesize, bool enforce_minimum = true);
    Config(bool enforce_minimum = true);

    static constexpr size_t min_fs_block_bytesize = 2 * 1024 * 1024;

    unsigned concurrency;
    size_t s3_block_bytesize;
    size_t fs_block_bytesize;
};

std::ostream & operator<<(std::ostream &, const Config &);

}; // namespace runai::llm::streamer::impl
