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

// Holds all tasks assigned to a single worker

struct WorkerTasks
{
    std::vector<FileReadTask> tasks;
    size_t total_bytes = 0; // Total bytes assigned to this worker
};

// Distributes multi-file read workload across workers

struct Assigner
{
    Assigner(
        // Input file descriptions
        const std::vector<std::string>& paths,
        const std::vector<size_t>& file_offsets,
        const std::vector<size_t>& bytesizes,
        const std::vector<void*>& dsts,
        std::shared_ptr<const Config> config);

    // Access the assignments for each worker
    const std::vector<FileReadTask> & file_assignments(unsigned file_index) const;

    unsigned get_num_workers() const;

    unsigned num_workloads() const;

 private:
    size_t bytes_per_worker(size_t total_bytes_to_read, size_t & remainder_bytesize);
    bool check_object_storage(const std::vector<std::string> & paths) const;

 private:
    std::shared_ptr<const Config> _config;
    bool _is_object_storage;
    unsigned _num_workers;
    std::map<unsigned, std::vector<FileReadTask>> _assignments; // assigned work for each file (key is the file index)
    std::vector<WorkerTasks> _worker_assignments; // ordered by worker index
    unsigned _num_workloads;
};

} // namespace runai::llm::streamer::impl
