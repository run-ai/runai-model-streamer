
#pragma once

#include <atomic>
#include <memory>
#include <functional>
#include <string>

#include "common/response_code/response_code.h"

namespace runai::llm::streamer::impl
{

// Request represents a sub range in a file
// For each request the streamer will issue a correponding response when the sub range is ready
// Reading the sub range can be divided between several workers of the streamer, where each part is represented by a task
// The request is completed and ready to respond when all its tasks are finished
// The request fails if any of the tasks has reported an error

struct Request
{
    Request(size_t file_offset, unsigned index, unsigned tasks, size_t bytesize);

    // return true if all the request's range had been read
    bool finished(common::ResponseCode result = common::ResponseCode::Success);

    common::ResponseCode ret() const;

    // offset in file
    const size_t offset;

    const unsigned index;

    const size_t bytesize;

 private:
    // counter of unread chunks (each chunk is handled by a Task object)
    std::atomic<unsigned int> _tasks;

    std::atomic<common::ResponseCode> _ret = common::ResponseCode::Success;
};

}; // namespace runai::llm::streamer::impl
