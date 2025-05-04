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
#include "streamer/impl/assigner/file_read_task/file_read_task.h"

namespace runai::llm::streamer::impl
{

// Transforms a file read request into batches, one batch per process
// Batches is a group of Batch objects, which together read the same file

struct Batches
{
    Batches(unsigned file_index,
           const std::vector<FileReadTask> & file_read_tasks,
           std::shared_ptr<const Config> config,
           std::shared_ptr<common::Responder> responder,
           const std::string & path,
           const common::s3::S3ClientWrapper::Params & params,
           size_t file_offset,
           size_t bytesize,
           const std::vector<size_t> & internal_sizes);

    Batches(Batches &&) = default;
    Batches & operator=(Batches &&) = default;

    unsigned size() const;

    Batch & operator[](unsigned index);

    size_t total() const;

 private:
    struct BatchItr
    {
        BatchItr(const std::vector<FileReadTask> & file_read_tasks);

        unsigned current_index() const;
        unsigned current_worker_index() const;
        size_t consume(size_t bytesize);

        const FileReadTask & current_read_task() const;
        const FileReadTask & read_task(unsigned i) const;

        unsigned workers() const;
        unsigned worker_index(unsigned index) const;

     private:
        const std::vector<FileReadTask> & _file_read_tasks;
        const unsigned _num_batches;
        unsigned _current_task_index;
        unsigned _current_worker_index;
        size_t _current_worker_bytesize;
    };

    // create all the tasks
    void build_tasks(std::shared_ptr<const Config> config, const std::string & path, const common::s3::S3ClientWrapper::Params & params, size_t file_offset, const std::vector<size_t> & internal_sizes);

    // create tasks of a given request
    void handle_request(std::vector<Tasks> & v_tasks, unsigned request_index, size_t request_file_offset, size_t request_size, char * destination);

    unsigned _file_index;

    unsigned _size;

    BatchItr _itr;

    std::vector<Batch> _batches;
    std::shared_ptr<common::Responder> _responder;

    size_t _total = 0;
};

} // namespace runai::llm::streamer::impl
