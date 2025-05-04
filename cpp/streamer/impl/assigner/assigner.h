#pragma once

#include <memory>
#include <string>
#include <vector>
#include <map>

#include "streamer/impl/assigner/file_read_task/file_read_task.h"
#include "streamer/impl/config/config.h"
#include "streamer/impl/batches/batches.h"

// Split reading from several files into several read assignments
// Each read assignment is destined to a worker, represented as FileReadTask
// FileReadTask will be later transformed to a Batch object

namespace runai::llm::streamer::impl
{

// Generates the flat list of logical Request objects
// including their destination base pointers.

// struct FileRequests
// {
//     FileRequests(
//         unsigned file_index,
//         const std::string & path,
//         size_t file_offset,
//         size_t bytesizes,
//         const std::vector<size_t> & internal_sizes,
//         void * dst, // single continous buffer to write to
//         const common::s3::Credentials& credentials);

//     unsigned file_index;
//     std::vector<std::shared_ptr<Request>> requests;
//     size_t total_bytes = 0;
// };


// Holds all tasks assigned to a single worker

struct WorkerTasks
{
    std::vector<FileReadTask> tasks;
    size_t total_bytes = 0; // Total bytes assigned to this worker
};

// Distributes multi-file read workload across workers

struct MultiFileWorkloadAssigner
{
    MultiFileWorkloadAssigner(
        // Input file descriptions
        const std::vector<std::string>& paths,
        const std::vector<size_t>& file_offsets,
        const std::vector<size_t>& bytesizes,
        const std::vector<void*>& dsts,
        std::shared_ptr<const Config> config);

    // Access the assignments for each worker
    const std::vector<FileReadTask> & file_assignments(unsigned file_index);

    unsigned get_num_workers() const;

 private:
    std::shared_ptr<const Config> _config;
    unsigned _num_workers;
    std::map<unsigned, std::vector<FileReadTask>> _assignments; // assigned work for each file (key is the file index)
    std::vector<WorkerTasks> _worker_assignments; // ordered by worker index
};

} // namespace runai::llm::streamer::impl
