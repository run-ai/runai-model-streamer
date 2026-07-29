
#pragma once

#include <atomic>
#include <map>
#include "streamer/impl/batch/batch.h"
#include "common/response_code/response_code.h"

namespace runai::llm::streamer::impl
{

// Workload - a validated container of batches (one per file) that share a backend kind. It is otherwise
// stateless: filesystem workloads are read synchronously by execute(); object-storage workloads are handed
// to the ObjectStorageWorker pool, which reads the batches via batches() and owns the async scheduling.
struct Workload
{
    Workload() = default;
    Workload(Workload &&) = default;
    Workload & operator=(Workload &&) = default;

    // Read a filesystem workload synchronously. NOT for object storage (asserts): those are dispatched to
    // the ObjectStorageWorker pool.
    void execute(std::atomic<bool> & stopped);

    common::ResponseCode add_batch(Batch && batch);

    // Fail all of this workload's batches with the given code, pushing an error response for each
    // of their sub-ranges (whichever task of a sub-range finishes last pushes the single response,
    // so this composes correctly with sub-ranges partly covered by other, dispatched workloads).
    // Used to drain a workload that could not be dispatched (see Streamer::async_request).
    void fail(common::ResponseCode code);

    size_t size() const;

    bool is_object_storage() const;

    // The batches, keyed by file index. Exposed (rather than befriending the worker) so ObjectStorageWorker
    // can chunk the tasks and route completions; non-const because completion handling mutates the batches
    // (handle_response / handle_error). add_batch stays the validated way to add one, and is always called
    // (populating the workload) before batches() is read - the workload is fully built at dispatch time.
    std::map<unsigned, Batch> & batches();
    const std::map<unsigned, Batch> & batches() const;

 private:
    common::ResponseCode verify_batch(const Batch & batch);

    std::map<unsigned, Batch> _batches_by_file_index;
    bool _is_object_storage = false;
};

}; // namespace runai::llm::streamer::impl
