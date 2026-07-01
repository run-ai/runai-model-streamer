#include "streamer/impl/streamer/streamer.h"

#include <fnmatch.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "utils/logging/logging.h"
#include "utils/scope_guard/scope_guard.h"

#include "streamer/impl/workload/workload.h"
#include "streamer/impl/assigner/assigner.h"
#include "common/exception/exception.h"
#include "common/storage_uri/storage_uri.h"

namespace runai::llm::streamer::impl
{

Streamer::Streamer() : Streamer(Config())
{}

Streamer::Streamer(Config config) :
    _config(std::make_shared<Config>(config)),
    _pool([&](Workload&& workload, std::atomic<bool> & stopped)
        {
            workload.execute(stopped);
        }, _config->max_concurrency())
{
    LOG(DEBUG) << config;
}

Streamer::~Streamer()
{
    try
    {
        LOG(DEBUG) << "Streamer shutting down";
    }
    catch(...)
    {}
}

common::ResponseCode Streamer::sync_read(const std::string & path, size_t file_offset, size_t bytesize, void * dst, const common::s3::Credentials & credentials)
{
    LOG(SPAM) << "Requested to read " << bytesize << " bytes from " << path << " offset " << file_offset;

    auto r = async_read(path, file_offset, bytesize, dst, 1, &bytesize, credentials);
    if (r != common::ResponseCode::Success)
    {
        return r;
    }

    return _responder->pop().ret;
}

common::ResponseCode Streamer::async_read(const std::string & path, size_t file_offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes, const common::s3::Credentials & credentials)
{
    common::ResponseCode ret = common::ResponseCode::Success;

    try
    {
        std::vector<std::string> paths;
        std::vector<size_t> file_offsets;
        std::vector<size_t> bytesizes;
        std::vector<void *> dsts;
        std::vector<unsigned> num_sizes_v;
        std::vector<std::vector<size_t>> internal_sizes_vv;

        paths.push_back(path);
        file_offsets.push_back(file_offset);
        bytesizes.push_back(bytesize);
        dsts.push_back(dst);
        num_sizes_v.push_back(num_sizes);

        std::vector<size_t> internal_sizes_v(internal_sizes, internal_sizes + num_sizes);

        internal_sizes_vv.push_back(internal_sizes_v);

        async_request(paths, file_offsets, bytesizes, dsts, num_sizes_v, internal_sizes_vv, credentials);
    }
    catch(const common::Exception & e)
    {
        LOG(ERROR) << "caught exception " << e.what();
        ret = e.error();
    }

    return ret;
}

common::Response Streamer::response()
{
    if (_responder == nullptr)
    {
        return common::Response(common::ResponseCode::FinishedError);
    }

    return _responder->pop();
}

common::ResponseCode Streamer::async_request(
    std::vector<std::string> & paths,
    std::vector<size_t> & file_offsets,
    std::vector<size_t> & bytesizes,
    std::vector<void *> & dsts,
    std::vector<unsigned> & num_sizes,
    std::vector<std::vector<size_t>> & internal_sizes,
    const common::s3::Credentials & credentials)
{
    // verify input
    verify_requests(paths, file_offsets, bytesizes, num_sizes, dsts);

    auto total_sizes =  std::accumulate(num_sizes.begin(), num_sizes.end(), 0u);

    if (_responder && !_responder->finished())
    {
        LOG(ERROR) << "Previous request is still running";
        throw common::Exception(common::ResponseCode::BusyError);
    }

    // expecting for total of num_sizes responses
    _responder = std::make_shared<common::Responder>(total_sizes);

    // cancel responder in case of an error - cancelled response will not delay sending the next request
    utils::ScopeGuard __responder_release([&](){_responder->cancel();});


    // divide reading between workers
    Assigner assigner(paths, file_offsets, bytesizes, dsts, _config);

    std::vector<Workload> workloads(assigner.num_workloads());

    // Create batches for each file

    for (size_t i = 0; i < paths.size(); ++i)
    {
        auto params = handle_s3(i, paths[i], credentials);
        LOG(DEBUG) << "Creating batches for file index " << i << " path: " <<  paths[i];
        Batches batches(i, assigner.file_assignments(i), _config, _responder, paths[i], params, internal_sizes[i]);
        const auto num_batches = batches.size();
        LOG(DEBUG) << "Created " << num_batches << " batches for file index " << i;
        for (size_t j = 0; j < num_batches; ++j)
        {
            auto & batch = batches[j];
            if (batch.tasks.size() == 0)
            {
                LOG(WARNING) << "Found empty batch " << batch;
                continue;
            }

            const auto worker_index = batch.worker_index;

            LOG(DEBUG) << "Batch: file index " << batch.file_index << " with " << batch.tasks.size() << " tasks for worker " << worker_index << " total bytes " << batch.total_bytes();

            const auto & result = workloads[worker_index].add_batch(std::move(batch));
            LOG(DEBUG) << "Added batch to worker " << worker_index << " with result " << result;
            if (result != common::ResponseCode::Success)
            {
                LOG(ERROR) << "Failed to add batch to worker " << worker_index << " error: " << result;
                return result;
            }
        }
    }

    // send batches to threadpool
    for (auto & workload : workloads)
    {
        if (workload.size() > 0)
        {
            LOG(DEBUG) << "sending workload to worker with batches " << workload.size();

            _pool.push(std::move(workload));
        }
    }

    __responder_release.cancel();

    return common::ResponseCode::Success;
}

void Streamer::verify_requests(std::vector<std::string> & paths, std::vector<size_t> & file_offsets, std::vector<size_t> & bytesizes, std::vector<unsigned> & num_sizes, std::vector<void *> & dsts)
{
    if (dsts[0] == 0)
    {
        LOG(ERROR) << "Destination buffer is null";
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }

    for (size_t i = 0; i < paths.size(); ++i)
    {
        LOG(SPAM) << "Requested to read asynchronously " << bytesizes[i] << " bytes from " << paths[i] << " offset " << file_offsets[i] << " in " << num_sizes[i] << " chunks";

        if (bytesizes[i] == 0 && num_sizes[i] == 0)
        {
            LOG(ERROR) << "Empty request - no response will be sent";
            throw common::Exception(common::ResponseCode::EmptyRequestError);
        }

        if (num_sizes[i] == 0 || bytesizes[i] == 0)
        {
            LOG(ERROR) << "Total bytes to read is " << bytesizes[i] << " but number of sub requests is " << num_sizes[i];
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }
    }
}

std::shared_ptr<common::s3::StorageUri> Streamer::try_parse_uri(const std::string & path)
{
    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_shared<common::s3::StorageUri>(path);
    }
    catch(const std::exception& e)
    {
    }
    return uri;
}

common::s3::S3ClientWrapper::Params Streamer::handle_s3(unsigned file_index, const std::string & path, const common::s3::Credentials & credentials)
{
    auto uri = try_parse_uri(path);

    if (uri != nullptr && _s3 == nullptr)
    {
        // adjust fd limit acording to concurrency
        auto fd_limit = utils::get_cur_file_descriptors();
        LOG(DEBUG) << "Process file descriptors limit is " << fd_limit << " and concurrency level is " << _config->s3_concurrency;
        const auto desired_fd_limit = _config->s3_concurrency * 64;
        if (fd_limit < desired_fd_limit)
        {
            if (desired_fd_limit > utils::get_max_file_descriptors())
            {
                LOG(ERROR) << "Insufficient file descriptors limit " << fd_limit << " for concurrency level " << _config->s3_concurrency << " ; increase fd limit to " << desired_fd_limit << " or higher, depending on your application fd usage";
                throw common::Exception(common::ResponseCode::InsufficientFdLimit);
            }
            LOG(INFO) << "Increasing fd soft limit to " << desired_fd_limit << " for concurrency level " << _config->s3_concurrency;
            _fd_limit = std::make_unique<utils::FdLimitSetter>(desired_fd_limit);
        }
        _s3_stop = std::make_unique<S3Stop>();
        _s3 = std::make_unique<S3Cleanup>();
    }

    return common::s3::S3ClientWrapper::Params(uri, credentials, _config->s3_block_bytesize);
}

std::vector<std::pair<std::string, size_t>> Streamer::list_files(
    const std::string & prefix,
    bool is_recursive,
    const std::vector<std::string> & allow_patterns,
    const std::vector<std::string> & ignore_patterns,
    const common::s3::Credentials & credentials)
{
    // fnmatch(3) filtering, matching the behavior of Python fnmatch.fnmatch(path, pattern)
    auto keep = [&](const char * path) -> bool
    {
        if (!allow_patterns.empty())
        {
            bool matched = false;
            for (const auto & p : allow_patterns)
            {
                if (fnmatch(p.c_str(), path, 0) == 0) { matched = true; break; }
            }
            if (!matched) return false;
        }
        for (const auto & p : ignore_patterns)
        {
            if (fnmatch(p.c_str(), path, 0) == 0) return false;
        }
        return true;
    };

    std::vector<std::pair<std::string, size_t>> results;

    auto uri = try_parse_uri(prefix);

    if (uri != nullptr)
    {
        // Any code path that uses the S3 plugin must release its clients and the
        // backend handle when the streamer shuts down (~Streamer). stop() and the
        // fd limit are streaming-only: listing is a single synchronous call with no
        // in-flight async reads and no threadpool workers to unblock.
        if (_s3 == nullptr)
        {
            _s3 = std::make_unique<S3Cleanup>();
        }

        common::s3::S3ClientWrapper::Params params(uri, credentials, _config->s3_block_bytesize);
        common::s3::S3ClientWrapper wrapper(params);

        common::backend_api::ObjectFileEntry_t * entries = nullptr;
        unsigned num_entries = 0;
        auto ret = wrapper.list_files(prefix.c_str(), is_recursive ? 1 : 0, &entries, &num_entries);
        if (ret != common::ResponseCode::Success)
        {
            throw common::Exception(ret); // entries is not allocated on error
        }

        // free_file_list is a no-op for nullptr (empty listing); ScopeGuard is
        // constructed only on the success path so it never fires on error.
        utils::ScopeGuard guard([&]{ wrapper.free_file_list(entries, num_entries); });

        for (unsigned i = 0; i < num_entries; ++i)
        {
            if (keep(entries[i].path))
            {
                results.emplace_back(entries[i].path, entries[i].size);
            }
        }
    }
    else
    {
        namespace fs = std::filesystem;
        const fs::path root(prefix);
        if (!fs::exists(root))
        {
            throw common::Exception(common::ResponseCode::FileAccessError);
        }

        auto process = [&](const fs::directory_entry & entry)
        {
            if (entry.is_regular_file() && keep(entry.path().c_str()))
            {
                results.emplace_back(entry.path().string(), static_cast<size_t>(entry.file_size()));
            }
        };

        if (is_recursive)
        {
            for (const auto & e : fs::recursive_directory_iterator(root)) process(e);
        }
        else
        {
            for (const auto & e : fs::directory_iterator(root)) process(e);
        }
    }

    return results;
}

}; // namespace runai::llm::streamer::impl
