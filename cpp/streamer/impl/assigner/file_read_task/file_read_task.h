#pragma once

#include <string>

namespace runai::llm::streamer::impl
{

// Represents a single contiguous read operation from a specific file

struct FileReadTask
{
    FileReadTask(unsigned worker_index,
                 unsigned file_idx,
                 const std::string& p,
                 size_t offset,
                 size_t sz,
                 char * dst);

    FileReadTask(FileReadTask&&) = default;
    FileReadTask& operator=(FileReadTask&&) = default;

    unsigned worker_index;

    unsigned original_file_index; // Index from the input vectors
    std::string path;             // Path of the file to read
    size_t offset_in_file;        // Starting byte offset within this file
    size_t size;                  // Number of bytes to read
    char * destination;            // Pointer to the destination buffer for this task's data
};

} // namespace runai::llm::streamer::impl
