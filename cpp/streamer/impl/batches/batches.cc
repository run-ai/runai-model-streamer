#include "streamer/impl/batches/batches.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <utility>
#include <map>
#include <vector>

#include "utils/logging/logging.h"

#include "common/exception/exception.h"
#include "streamer/impl/file/file.h"
#include "streamer/impl/s3/s3.h"
#include "common/s3_wrapper/s3_wrapper.h"


namespace runai::llm::streamer::impl
{

Batches::BatchItr::BatchItr(const std::vector<FileReadTask> & file_read_tasks) :
     _file_read_tasks(file_read_tasks),
    _num_batches(file_read_tasks.size()),
    _current_task_index(0)
{
    ASSERT(_num_batches) << "Zero file read requests";
    _current_worker_index = file_read_tasks[_current_task_index].worker_index;
    _current_worker_bytesize = file_read_tasks[_current_task_index].size;
}

unsigned Batches::BatchItr::current_index() const
{
    return _current_task_index;
}

unsigned Batches::BatchItr::current_worker_index() const
{
    return _current_worker_index;
}

const FileReadTask & Batches::BatchItr::read_task(unsigned index) const
{
    ASSERT(index < _num_batches) << "Index overflow " << index << " should be less than " << _num_batches;
    return _file_read_tasks[index];
}

unsigned Batches::BatchItr::workers() const
{
    return _num_batches;
}

unsigned Batches::BatchItr::worker_index(unsigned index) const
{
    return read_task(index).worker_index;
}

const FileReadTask & Batches::BatchItr::current_read_task() const
{
    ASSERT(_current_task_index < _num_batches) << "Batches iterator overflow _current_task_index = " << _current_task_index << " num_batches = " << _num_batches;
    return _file_read_tasks[_current_task_index];
}

size_t Batches::BatchItr::consume(size_t bytesize)
{
    if (bytesize == 0)
    {
        LOG(DEBUG) << "consuming zero bytes request";
    }
    if (_current_worker_bytesize == 0 && bytesize)
    {
        // advance to the next worker
        ++_current_task_index;
        _current_worker_index = worker_index(_current_task_index);
        _current_worker_bytesize = read_task(_current_task_index).size;
    }

    auto to_read = std::min<size_t>(_current_worker_bytesize, bytesize);
    _current_worker_bytesize -= to_read;
    return to_read;
}

Batches::Batches(unsigned file_index,
                 const std::vector<FileReadTask> & file_read_tasks,
                 std::shared_ptr<const Config> config,
                 std::shared_ptr<common::Responder> responder,
                 const std::string & path,
                 const common::s3::S3ClientWrapper::Params & params,
                 const std::vector<size_t> & internal_sizes,
                 bool cuda,
                 std::vector<void*> cuda_tensor_dsts) :
    _file_index(file_index),
    _cuda(cuda),
    _cuda_tensor_dsts(std::move(cuda_tensor_dsts)),
    _itr(file_read_tasks),
    _responder(responder)
{
    _batches.reserve(file_read_tasks.size());
    build_tasks(config, path, params, internal_sizes);
}

unsigned Batches::size() const
{
    return _batches.size();
}

Batch & Batches::operator[](unsigned index)
{
    ASSERT(index < _batches.size()) << "Batches overflow ( index " << index << " > size " << _batches.size();
    return _batches[index];
}

size_t Batches::total() const
{
    return _total;
}

void Batches::build_tasks(std::shared_ptr<const Config> config, const std::string & path, const common::s3::S3ClientWrapper::Params & params, const std::vector<size_t> & internal_sizes)
{
    const auto num_workers = _itr.workers();
    LOG(DEBUG) << "Building tasks for " <<num_workers << " workers";
    std::vector<Tasks> v_tasks(num_workers);

    auto num_sizes = internal_sizes.size();
    size_t request_file_offset = _itr.read_task(0).offset_in_file;

    auto destination_start = static_cast<char *>(_itr.read_task(0).destination);

    auto current_request_destination = destination_start;

    // iterate over the workers and the requests to fill each worker share
    for (unsigned request_index = 0; request_index < num_sizes; ++request_index)
    {
        // create tasks for the entire requested range before sending to the threadpool
        const size_t request_size = internal_sizes[request_index];

        // For CUDA with pre-aligned per-tensor destinations, use the supplied pointer directly.
        // For CPU (or CUDA without per-tensor dsts), advance linearly through the buffer.
        char * destination = (_cuda && !_cuda_tensor_dsts.empty())
            ? static_cast<char*>(_cuda_tensor_dsts[request_index])
            : current_request_destination;

        handle_request(v_tasks, request_index, request_file_offset, request_size, destination);
        LOG(DEBUG) << "created request index " << request_index << " dst " << static_cast<void *>(destination);

        current_request_destination += request_size;
        request_file_offset += request_size;
    }

    for (unsigned i = 0; i < num_workers; ++i)
    {
        const auto worker_index = _itr.worker_index(i);
        auto & tasks = v_tasks[i];
        auto size = tasks.size();
        if (size == 0)
        {
            LOG(WARNING) << "Zero tasks for worker index " << _itr.worker_index(i);
            continue;
        }

        _batches.emplace_back(worker_index, _file_index, path, params, std::move(tasks), _responder, config, _cuda);
    }

    for (auto & batch : _batches)
    {
        _total += batch.total_bytes();
    }
}

void Batches::handle_request(std::vector<Tasks> & v_tasks, unsigned request_index, size_t request_file_offset, size_t request_size, char * destination)
{
    LOG(DEBUG) << "request file offset " << request_file_offset << " size " << request_size;

    // create tasks info

    // map worker index to tasks info
    std::map<unsigned, Task::Info> infos;

    auto bytes_to_request = request_size;
    size_t task_offset = request_file_offset;
    size_t destination_offset = 0;
    do
    {
        auto to_read = _itr.consume(bytes_to_request);
        Task::Info info(task_offset, to_read, destination_offset);
        auto worker_index = _itr.current_index();
        infos.try_emplace(worker_index, std::move(info));
        task_offset += to_read;
        bytes_to_request -= to_read;
        destination_offset += to_read;
    } while (bytes_to_request > 0);

    auto request_ptr = std::make_shared<Request>(request_file_offset, _file_index, request_index, infos.size(), request_size, destination);

    // create tasks
    for (auto & [batch_id, info] : infos)
    {
        Task task(request_ptr, std::move(info));
        LOG(SPAM) << task;
        ASSERT(batch_id < v_tasks.size()) << batch_id << " v_tasks.size() " << v_tasks.size();
        v_tasks[batch_id].emplace_back(std::move(task));
    }
}

}; // namespace runai::llm::streamer::impl
