#include "streamer/impl/assigner/file_read_task/file_read_task.h"

namespace runai::llm::streamer::impl
{

// Constructor
FileReadTask::FileReadTask(unsigned worker_index, unsigned file_idx, const std::string & p, size_t offset, size_t sz, char * dst) :
    worker_index(worker_index),
    original_file_index(file_idx),
    path(p),
    offset_in_file(offset),
    size(sz),
    destination(dst)
{}

} // namespace runai::llm::streamer::impl
