
#pragma once

#include <atomic>
#include <vector>
#include <map>
#include <memory>
#include <set>
#include "streamer/impl/batch/batch.h"
#include "streamer/impl/reader/reader.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/response_code/response_code.h"

namespace runai::llm::streamer::impl
{

struct Workload
{
    Workload() = default;
    Workload(Workload &&) = default;
    Workload & operator=(Workload &&) = default;

    void execute(std::atomic<bool> & stopped);

    common::ResponseCode add_batch(Batch && batch);

    size_t size() const;

    bool is_object_storage() const;

 private:
    common::ResponseCode verify_batch(const Batch & batch);
    void wait_for_responses(std::atomic<bool> & stopped);
    void async_read(std::atomic<bool> & stopped);
    common::ResponseCode handle_batch(unsigned file_index, Batch & batch, std::atomic<bool> & stopped);
    void assign_global_ids();
 private:
    std::map<unsigned, Batch> _batches_by_file_index;
    std::map<unsigned, common::ResponseCode> _error_by_file_index;
    bool _is_object_storage = false;
    std::shared_ptr<Reader> _reader;
    size_t _total_tasks = 0;
    static std::atomic<common::backend_api::ObjectRequestId_t> _async_handle_counter;
    common::backend_api::ObjectRequestId_t _global_id_base;
    std::vector<const Task*> _tasks;
};

}; // namespace runai::llm::streamer::impl
