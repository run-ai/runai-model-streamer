#pragma once

#include <memory>
#include <string>
#include <vector>

#include "streamer/impl/assigner/file_read_task/file_read_task.h"
#include "streamer/impl/config/config.h"
#include "streamer/impl/batches/batches.h"
#include "streamer/impl/request/request.h"

// Turn a multi-file read request into read assignments:
//   1. coalesce each file's ranges into ContiguousTransfers
//   2. divide the transfers between workers, each slice represented as a FileReadTask
// A FileReadTask is later transformed into a Batch object

namespace runai::llm::streamer::impl
{

// ContiguousTransfer - a maximal group of the caller's ranges that are adjacent in BOTH the file and
// the destination buffer, so the whole group can be served by one sequential read. A transfer never
// spans two files.
//
// Coalescing happens BEFORE any work is divided between workers, so a single-worker streamer still
// reads a coalesced group with one read rather than one read per range.
//
// Ranges are coalesced in the order the caller supplied them: a caller that lists a file's ranges by
// ascending offset gets maximal coalescing (the .cc logs at DEBUG when they arrive unordered, so the
// lost coalescing is visible rather than silent).
//
// The transfer is also the unit that O_DIRECT alignment is priced per - each one needs its own
// block-aligned destination region - so coalescing bounds alignment overhead as well as seeks.

struct ContiguousTransfer
{
    unsigned file_index = 0;

    size_t offset = 0;             // start offset in the file
    size_t size = 0;               // total bytes of all the ranges it covers
    char * destination = nullptr;  // destination of its first range

    // Index of its first range within the file. The ranges it covers are
    // [first_range_index, first_range_index + range_sizes.size()), which is what makes the response
    // index computable: the j-th range of this transfer is request index first_range_index + j.
    unsigned first_range_index = 0;

    // Sizes of the ranges it covers, in order. Batches turns these into Request objects - one
    // response each, whatever the size.
    std::vector<size_t> range_sizes;

    // This transfer's slices, one per worker it was divided between (usually one)
    std::vector<FileReadTask> tasks;
};

// Holds all tasks assigned to a single worker

struct WorkerTasks
{
    std::vector<FileReadTask> tasks;
    size_t total_bytes = 0; // Total bytes assigned to this worker
};

// Distributes multi-file read workload across workers

struct Assigner
{
    Assigner(const std::vector<FileRanges> & request, std::shared_ptr<const Config> config);

    // The coalesced transfers, each carrying the worker slices it was divided into. Batches is built
    // per transfer: within a transfer the ranges tile one contiguous span of file and buffer, which is
    // the assumption Batch is built on.
    const std::vector<ContiguousTransfer> & transfers() const;

    unsigned get_num_workers() const;

    unsigned num_workloads() const;

 private:
    size_t bytes_per_worker(size_t total_bytes_to_read, size_t & remainder_bytesize);
    bool check_object_storage(const std::vector<FileRanges> & request) const;

    // Step 1 - merge each file's ranges that are adjacent in both file and destination. Independent of
    // the worker count, and of how the work is later divided. Returns the total bytes of the request,
    // accumulated during the same pass so the ranges are walked only once.
    size_t coalesce(const std::vector<FileRanges> & request);

    // Step 2 - divide _transfers between the workers by total bytes, splitting a transfer that is
    // larger than a worker's remaining target. Offset and destination advance together across a split,
    // so each slice stays contiguous.
    void assign(size_t total_bytes_to_read);

 private:
    std::shared_ptr<const Config> _config;
    bool _is_object_storage;
    unsigned _num_workers;
    std::vector<ContiguousTransfer> _transfers;
    std::vector<WorkerTasks> _worker_assignments; // ordered by worker index
    unsigned _num_workloads;
};

} // namespace runai::llm::streamer::impl
