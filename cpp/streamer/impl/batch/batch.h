
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/storage_uri/storage_uri.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/response_code/response_code.h"
#include "common/responder/responder.h"
#include "common/range/range.h"

#include "streamer/impl/config/config.h"
#include "streamer/impl/task/task.h"
#include "streamer/impl/reader/reader.h"

namespace runai::llm::streamer::impl
{

// Batch represents a single range in the file, and is used by a single worker thread
// The range may contain several sub ranges (requests) which can be whole or partial
// Each sub range (or part of it) is represented by a task
//
//         [                Batch range                     ]
//   [ ... request 1  ][request 2][   request 3   ][ request 4 ....]
//         [task 1    ][  task 2 ][    task 3     ][ task 4 ]

using Tasks = std::vector<Task>;

struct Range : common::Range
{
    Range() = default;
    Range(size_t start_offset, size_t end_offset);
    Range(const Tasks & tasks);

    size_t end;

 private:
    static size_t calculate_start(const Tasks & tasks);
    static size_t calculate_end(const Tasks & tasks);
};

struct Batch
{
    Batch() = default;
    Batch(Batch &&) = default;
    Batch & operator=(Batch &&) = default;

    Batch(const std::string & path, const common::s3::S3ClientWrapper::Params & params, Range && range, char * dst, const Tasks && tasks, std::shared_ptr<common::Responder> responder, std::shared_ptr<const Config> config);

    void execute(std::atomic<bool> & stopped);

    // notify tasks until file offset
    void finished_until(size_t file_offset, common::ResponseCode ret = common::ResponseCode::Success);
    unsigned finished_until() const;

    std::string path;

    // s3 parameters
    common::s3::S3ClientWrapper::Params params;

    // range in file
    Range range;

    // start offset in destination buffer
    char * dst;

    Tasks tasks;

    std::shared_ptr<common::Responder> responder;

    std::shared_ptr<const Config> config;

 private:
    void read(const Config & config, std::atomic<bool> & stopped);
    void async_read(const Config & config, std::atomic<bool> & stopped);

 private:
    // index of first unfinished task
    unsigned _unfinished = 0;

    std::unique_ptr<Reader> _reader;
};

}; // namespace runai::llm::streamer::impl
