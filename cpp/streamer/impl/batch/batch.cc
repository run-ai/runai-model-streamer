#include "streamer/impl/batch/batch.h"

#include <algorithm>
#include <memory>
#include <utility>
#include <set>
#include <map>
#include <vector>

#include "utils/logging/logging.h"

#include "common/exception/exception.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/range/range.h"
#include "streamer/impl/reader/reader.h"
#include "streamer/impl/file/file.h"
#include "streamer/impl/s3/s3.h"

namespace runai::llm::streamer::impl
{

Range::Range(size_t start_offset, size_t end_offset) :
    common::Range(start_offset, end_offset - start_offset),
    end(end_offset)
{
    if (end < start)
    {
        LOG(ERROR) << "Invalid range " << start << " - " << end;
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }
}

Range::Range(const Tasks & tasks) :
    Range(calculate_start(tasks), calculate_end(tasks))
{}

size_t Range::calculate_start(const Tasks & tasks)
{
    if (tasks.empty())
    {
        return 0;
    }

    return tasks[0].info.offset;
}

size_t Range::calculate_end(const Tasks & tasks)
{
    if (tasks.empty())
    {
        return 0;
    }

    return tasks[tasks.size() - 1].info.end;
}

Batch::Batch(const std::string & path, std::shared_ptr<common::s3::StorageUri> uri, Range && range, char * dst, const Tasks && tasks, std::shared_ptr<common::Responder> responder, std::shared_ptr<const Config> config) :
    path(path),
    uri(uri),
    range(range),
    dst(dst),
    tasks(tasks),
    responder(responder),
    config(config)
{
    LOG(SPAM) << "Batch " << path << " range " << this->range.start << " - " << this->range.end << " ; " << this->tasks.size() << " tasks";
}

void Batch::execute(std::atomic<bool> & stopped)
{
    LOG(DEBUG) << "Start reading from file " << path;

    auto response_code = common::ResponseCode::Success;
    try
    {
        // create reader
        if (uri.get() != nullptr)
        {
            auto s3_client = std::make_shared<common::s3::S3ClientWrapper>(*uri);
            _reader = std::make_unique<S3>(s3_client, *config);
        }
        else
        {
            _reader = std::make_unique<File>(path, *config);
        }

        if (_reader->mode == Reader::Mode::Sync)
        {
            read(*config, stopped);
        }
        else
        {
            async_read(*config, stopped);
        }

        if (stopped)
        {
            throw common::Exception(common::ResponseCode::FinishedError);
        }
    }
    catch(const common::Exception & e)
    {
        response_code = e.error();
    }
    catch (...)
    {
        response_code = common::ResponseCode::UnknownError;
    }

    // in case of an error all of the batch's unfinished tasks are failed with the same error code
    // in case of success the finished tasks were already notified

    if (response_code != common::ResponseCode::Success)
    {
        LOG(ERROR) << "Failed to read from file " << path << " ; error: " << response_code;
        finished_until(range.end, response_code);
    }
}

// read the entire range and send notifications for each sub range
void Batch::read(const Config & config, std::atomic<bool> & stopped)
{
    if (tasks.empty())
    {
        LOG(DEBUG) << "Empty batch";
        return;
    }

    auto file_offset = range.start;
    char * buffer = dst;
    size_t num_chunks = range.size / config.fs_block_bytesize;

    // seek just once because tasks are consecutive within the range
    _reader->seek(file_offset);

    // read task's range in chunks
    for (size_t i = 0; i < num_chunks && !stopped; ++i)
    {
        _reader->read(config.fs_block_bytesize, buffer);

        file_offset += config.fs_block_bytesize;
        buffer += config.fs_block_bytesize;

        finished_until(file_offset, common::ResponseCode::Success);
    }

    if (file_offset < range.end && !stopped)
    {
        _reader->read(range.end - file_offset, buffer);
        finished_until(range.end, common::ResponseCode::Success);
    }

    LOG(DEBUG) << "Finished reading from file " << path << (stopped ? " - interrupted" : " successfully");
}

// read the entire range and send notifications for each sub range
void Batch::async_read(const Config & config, std::atomic<bool> & stopped)
{
    if (tasks.empty())
    {
        LOG(DEBUG) << "Empty batch";
        return;
    }

    if (stopped)
    {
        return;
    }

    std::vector<common::Range> ranges;
    ranges.reserve(tasks.size());

    for (size_t i = 0; i < tasks.size(); ++i)
    {
        ranges.push_back({tasks[i].info.offset, tasks[i].info.bytesize});
    }

    _reader->async_read(ranges, dst);

    while (true && !stopped)
    {
        auto r = _reader->async_response();

        if (r.ret == common::ResponseCode::FinishedError)
        {
            break;
        }

        LOG(SPAM) << "Received response index " << r.index;
        if (r.index < 0 || r.index >= tasks.size())
        {
            LOG(ERROR) << "received out of range index " << r.index << " number of tasks is " << tasks.size();
        }
        const auto & task = tasks.at(r.index);
        if (task.request->finished(r.ret))
        {
            common::Response response(task.request->index, task.request->ret());
            responder->push(std::move(response), task.request->bytesize);
        }
    }

    LOG(DEBUG) << "Finished reading from file " << path << (stopped ? " - interrupted" : " successfully");
}

// notify unfinished tasks up to but not including offset end
void Batch::finished_until(size_t file_offset, common::ResponseCode ret /*= common::ResponseCode::Success */)
{
    unsigned i = _unfinished;
    for (; i < tasks.size(); ++i)
    {
        if (file_offset < tasks[i].info.end)
        {
            break;
        }
        if (tasks[i].request->finished(ret))
        {
            const auto & r = tasks[i].request;
            common::Response response(r->index, r->ret());
            responder->push(std::move(response), tasks[i].request->bytesize);
        }
    }
    _unfinished = i;
}

unsigned Batch::finished_until() const
{
    return _unfinished;
}

}; // namespace runai::llm::streamer::impl
