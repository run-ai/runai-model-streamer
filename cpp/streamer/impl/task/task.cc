
#include "streamer/impl/task/task.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

Task::Info::Info(size_t offset, size_t bytesize) :
    offset(offset),
    bytesize(bytesize),
    end(offset + bytesize)
{}

Task::Task(std::shared_ptr<Request> request, size_t offset, size_t bytesize) :
    Task(request, Info(offset, bytesize))
{}

Task::Task(std::shared_ptr<Request> request, Info && info) :
    request(request),
    info(info)
{}

std::ostream & operator<<(std::ostream & os, const Task & task)
{
    os << "task to read " << task.info.bytesize << " bytes from file offset " << task.info.offset << " to " << task.info.end;
    return os;
}


}; // namespace runai::llm::streamer::impl
