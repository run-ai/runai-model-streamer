
#pragma once

#include <atomic>
#include <vector>
#include <map>
#include <memory>
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

 private:
    common::ResponseCode verify_batch(const common::s3::S3ClientWrapper::Params & params);
    void wait_for_responses(std::atomic<bool> & stopped);
    void async_read(std::atomic<bool> & stopped);
    common::ResponseCode handle_batch(unsigned file_index, Batch & batch, std::atomic<bool> & stopped);
 private:
    std::map<unsigned, Batch> _batches_by_file_index;
    std::map<unsigned, common::ResponseCode> _error_by_file_index;
    common::s3::S3ClientWrapper::Params _params;
    std::shared_ptr<Reader> _reader;
};

}; // namespace runai::llm::streamer::impl
