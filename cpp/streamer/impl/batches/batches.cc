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

Batches::BatchItr::BatchItr(unsigned num_batches, size_t worker_bytesize) :
    _num_batches(num_batches),
    _worker_bytesize(worker_bytesize),
    _current_worker_bytesize(_worker_bytesize)
{}

unsigned Batches::BatchItr::current_index() const
{
    return _current_worker_index;
}

size_t Batches::BatchItr::worker_bytesize() const
{
    return _worker_bytesize;
}

size_t Batches::BatchItr::consume(size_t bytesize)
{
    if (_current_worker_bytesize == 0)
    {
        // advance to the next worker
        ++_current_worker_index;
        _current_worker_bytesize = _worker_bytesize;
    }

    ASSERT(_current_worker_index < _num_batches) << "Batches iterator overflow";

    auto to_read = std::min<size_t>(_current_worker_bytesize, bytesize);
    _current_worker_bytesize -= to_read;
    return to_read;
}

Batches::Batches(std::shared_ptr<const Config> config, std::shared_ptr<common::Responder> responder, const std::string & path, std::shared_ptr<common::s3::StorageUri> uri, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes) :
    _itr(config->concurrency, batch_bytesize(bytesize, *config, uri)),
    _responder(responder)
{
    LOG(DEBUG) << "worker maximal range size is " << utils::logging::human_readable_size(_itr.worker_bytesize());
    _batches.reserve(config->concurrency);
    build_tasks(config, path, uri, file_offset, dst, num_sizes, internal_sizes);
}

unsigned Batches::size() const
{
    return _batches.size();
}

size_t Batches::batch_bytesize(size_t bytesize, const Config & config, std::shared_ptr<common::s3::StorageUri> uri)
{
    size_t result = std::ceil(static_cast<double>(bytesize) / static_cast<double>(config.concurrency));

    // round up to the configured chunk byte size
    const auto chunk_bytesize = (uri.get() == nullptr ? config.fs_block_bytesize : config.s3_block_bytesize);
    int remainder = result % chunk_bytesize;
    if (remainder)
    {
        result += (chunk_bytesize - remainder);
    }

    return result;
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

void Batches::build_tasks(std::shared_ptr<const Config> config, const std::string & path, std::shared_ptr<common::s3::StorageUri> uri, size_t file_offset, void * dst, unsigned num_sizes, size_t * internal_sizes)
{
    std::vector<Tasks> v_tasks(config->concurrency);
    std::vector<Range> v_ranges(config->concurrency);

    size_t * size_ptr = internal_sizes;
    size_t request_file_offset = file_offset;

    // iterate over the workers and the requests to fill each worker share
    for (unsigned request_index = 0; request_index < num_sizes; ++request_index)
    {
        // create tasks for the entire requested range before sending to the threadpool
        const size_t request_size = *size_ptr;

        handle_request(v_tasks, request_index, request_file_offset, request_size);

        request_file_offset += request_size;
        ++size_ptr;
    }

    auto dst_ = static_cast<char *>(dst);

    for (unsigned i = 0; i < config->concurrency; ++i)
    {
        auto & tasks = v_tasks[i];
        auto size = tasks.size();
        if (size == 0)
        {
            break;
        }

        auto range = Range(tasks);
        _total += range.size;

        const auto range_size = range.size;

        _batches.emplace_back(path, uri, std::move(range), dst_, std::move(tasks), _responder, config);

        dst_ += range_size;
    }
}

void Batches::handle_request(std::vector<Tasks> & v_tasks, unsigned request_index, size_t request_file_offset, size_t request_size)
{
    LOG(DEBUG) << "request file offset " << request_file_offset << " size " << request_size;

    // create tasks info
    std::map<unsigned, Task::Info> infos;

    auto bytes_to_request = request_size;
    size_t task_offset = request_file_offset;

    while (bytes_to_request > 0)
    {
        auto to_read = _itr.consume(bytes_to_request);
        Task::Info info(task_offset, to_read);
        infos.try_emplace(_itr.current_index(), std::move(info));
        task_offset += to_read;
        bytes_to_request -= to_read;
    }

    auto request_ptr = std::make_shared<Request>(request_file_offset, request_index, infos.size(), request_size);

    // create tasks
    for (auto & [batch_id, info] : infos)
    {
        Task task(request_ptr, std::move(info));
        LOG(SPAM) << task;
        v_tasks[batch_id].emplace_back(std::move(task));
    }
}

}; // namespace runai::llm::streamer::impl
