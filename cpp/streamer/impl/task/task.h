
#pragma once

#include <memory>

#include "streamer/impl/request/request.h"

namespace runai::llm::streamer::impl
{

// Task represents a chunk which is read by a single worker thread, and belongs to a single request (i.e. to a single sub range in the file)
// Each request consists of one or several tasks, where each task is read by a different thread

struct Task
{
    // file offsets to read
    struct Info
    {
        Info(size_t offset, size_t bytesize);
        size_t offset;
        size_t bytesize;
        size_t end;
    };

    Task(std::shared_ptr<Request> request, Info && info);
    Task(std::shared_ptr<Request> request, size_t offset, size_t bytesize);

    bool finished_request(common::ResponseCode ret);

    std::shared_ptr<Request> request;
    Info info;

    // Destination information for the worker
    char * destination_base;      // Base address for the *request's* data buffer
    size_t destination_offset;   // Offset from destination_base where *this task* should write

 private:
    bool _finished = false;
};

std::ostream & operator<<(std::ostream &, const Task &);

}; // namespace runai::llm::streamer::impl
