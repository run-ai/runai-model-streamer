#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/responder/responder.h"
#include "common/storage_uri/storage_uri.h"
#include "streamer/impl/batch/batch.h"
#include "streamer/impl/config/config.h"
#include "streamer/impl/request/request.h"
#include "streamer/impl/reader/reader.h"

namespace runai::llm::streamer::impl
{

// Transforms a file read request into batches

struct Batches
{
    Batches(std::shared_ptr<const Config> config, std::shared_ptr<common::Responder> responder, const std::string & path, const common::s3::S3ClientWrapper::Params & params, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes);

    unsigned size() const;

    Batch & operator[](unsigned index);

    size_t total() const;

 private:
    struct BatchItr
    {
        BatchItr(unsigned num_batches, size_t worker_bytesize);

        size_t worker_bytesize() const;
        unsigned current_index() const;
        size_t consume(size_t bytesize);

     private:
        const unsigned _num_batches;
        unsigned _current_worker_index = 0;
        const size_t _worker_bytesize;
        size_t _current_worker_bytesize;
    };

    // calculate the range size of each batch
    size_t batch_bytesize(const size_t bytesize, const Config & config, std::shared_ptr<common::s3::StorageUri> uri);

    // create all the tasks
    void build_tasks(std::shared_ptr<const Config> config, const std::string & path, const common::s3::S3ClientWrapper::Params & params, size_t file_offset, void * dst, unsigned num_sizes, size_t * internal_sizes);

    // create tasks of a given request
    void handle_request(std::vector<Tasks> & v_tasks, unsigned request_index, size_t request_file_offset, size_t request_size);

    unsigned _size;

    BatchItr _itr;

    std::vector<Batch> _batches;
    std::shared_ptr<common::Responder> _responder;

    size_t _total = 0;
};

} // namespace runai::llm::streamer::impl
