#include "streamer/impl/batch/batch.h"

#include <gtest/gtest.h>
#include <utility>
#include <memory>
#include <chrono>
#include <set>

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/temp/file/file.h"
#include "utils/thread/thread.h"
#include "utils/dylib/dylib.h"
#include "utils/scope_guard/scope_guard.h"

#include "common/s3_wrapper/s3_wrapper.h"

#include "streamer/impl/file/file.h"

namespace runai::llm::streamer::impl
{

TEST(Batch, Finished_Until)
{
    unsigned num_tasks = utils::random::number(1, 10);
    const auto path = utils::random::string();
    common::s3::S3ClientWrapper::Params params;

    // File range to read
    auto start = utils::random::number<size_t>(0, 1024);
    auto size = utils::random::number<size_t>(num_tasks, 1024 * 1024);
    EXPECT_LT(num_tasks, size);

    // divide range into chunks - a chunk per task
    auto chunks = utils::random::chunks(size, num_tasks);

    auto responder = std::make_shared<common::Responder>(1);
    auto request = std::make_shared<Request>(start, utils::random::number(), num_tasks, size);

    // create tasks

    size_t offset = start;
    Tasks tasks;

    for (unsigned i = 0; i < num_tasks; ++i)
    {
        // task offset is relative to the beginning of the request offset
        auto task = Task(request, offset, chunks[i]);
        offset += chunks[i];
        tasks.push_back(std::move(task));
    }

    auto range = Range(tasks);
    auto expected_range = Range(start, start + size);
    EXPECT_EQ(range.start, expected_range.start);
    EXPECT_EQ(range.end, expected_range.end);
    EXPECT_EQ(range.size, expected_range.size);

    // create batch
    const auto config = std::make_shared<Config>();

    Batch batch(path, params, std::move(range), nullptr, std::move(tasks), responder, config);

    // execute part of the tasks

    auto mid_point = utils::random::number<size_t>(start, start + size);
    unsigned expected = 0;
    size_t total = start;
    while (total < mid_point)
    {
        total += chunks[expected];
        ++expected;
    }
    expected = (total > mid_point ? expected - 1 : expected);

    batch.finished_until(mid_point);

    EXPECT_EQ(batch.finished_until(), expected);

    EXPECT_FALSE(batch.responder->finished());

    // execute rest of the tasks

    batch.finished_until(expected_range.end);

    EXPECT_EQ(batch.finished_until(), num_tasks);

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::Success);
}

TEST(Read, Sanity)
{
    unsigned num_tasks = utils::random::number(1, 10);

    // File range to read
    const auto start = utils::random::number<size_t>(0, 1024);
    const auto size = utils::random::number<size_t>(num_tasks, 1024 * 1024);
    EXPECT_LT(num_tasks, size);

    auto range = Range(start, start + size);
    EXPECT_EQ(range.size, size);

    const auto data = utils::random::buffer(start + size);
    utils::temp::File file(data);
    const auto path = file.path;
    common::s3::S3ClientWrapper::Params params;

    // divide range into chunks - a chunk per task
    auto chunks = utils::random::chunks(range.size, num_tasks);

    auto responder = std::make_shared<common::Responder>(1);

    const auto chunk_bytesize = utils::random::number<size_t>(1, range.size);
    const auto config = std::make_shared<Config>(utils::random::number(1, 4), chunk_bytesize, utils::random::number<size_t>(1, chunk_bytesize), false /* do not force minimum chunk size */);

    std::vector<char> dst(size);
    auto dst_ptr = dst.data();

    // create tasks
    auto request = std::make_shared<Request>(range.start, utils::random::number(), num_tasks, size);

    size_t offset = start;

    Tasks tasks;
    for (unsigned i = 0; i < num_tasks; ++i)
    {
        // task offset is relative to the beginning of the request offset
        auto task = Task(request, offset, chunks[i]);
        offset += chunks[i];
        tasks.push_back(std::move(task));
    }

    Batch batch(path, params, std::move(range), dst_ptr, std::move(tasks), responder, config);

    std::atomic<bool> stopped(false);
    EXPECT_NO_THROW(batch.execute(stopped));

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::Success);

    // verify read data
    bool mismatch = false;
    for (size_t i = 0; i < size && !mismatch; ++i)
    {
        mismatch = dst[i] != static_cast<char>(data[start + i]);
    }
    EXPECT_FALSE(mismatch);
}

TEST(Read, Error)
{
    std::string path;
    unsigned num_tasks = utils::random::number(2, 10); // need at least two tasks in this test

    // File range to read
    auto start = utils::random::number<size_t>(0, 1024);
    auto size = utils::random::number<size_t>(num_tasks, 1024 * 1024);
    EXPECT_LT(num_tasks, size);

    auto range = Range(start, start + size);
    EXPECT_EQ(range.size, size);

    const auto data = utils::random::buffer(start + size - utils::random::number<size_t>(1, size));
    utils::temp::File file(data);
    path = file.path;
    common::s3::S3ClientWrapper::Params params;

    // divide range into chunks - a chunk per task
    auto chunks = utils::random::chunks(range.size, num_tasks);

    auto responder = std::make_shared<common::Responder>(1);

    const auto config = std::make_shared<Config>();

    std::vector<char> dst(size);
    auto dst_ptr = dst.data();

    // create tasks
    auto request = std::make_shared<Request>(range.start, utils::random::number(), num_tasks, size);

    size_t offset = start;

    Tasks tasks;

    for (unsigned i = 0; i < num_tasks; ++i)
    {
        // task offset is relative to the beginning of the request offset
        auto task = Task(request, offset, chunks[i]);
        offset += chunks[i];
        tasks.push_back(std::move(task));
    }

     Batch batch(path, params, std::move(range), dst_ptr, std::move(tasks), responder, config);

    std::atomic<bool> stopped(false);
    EXPECT_NO_THROW(batch.execute(stopped));

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::EofError);
}

TEST(Read, Already_Stopped)
{
    unsigned num_tasks = utils::random::number(1, 10);

    // File range to read
    auto start = utils::random::number<size_t>(0, 1024);
    auto size = utils::random::number<size_t>(num_tasks, 1024 * 1024);
    EXPECT_LT(num_tasks, size);

    auto range = Range(start, start + size);
    EXPECT_EQ(range.size, size);

    const auto data = utils::random::buffer(start + size);
    utils::temp::File file(data);
    const auto path = file.path;
    common::s3::S3ClientWrapper::Params params;

    // divide range into chunks - a chunk per task
    auto chunks = utils::random::chunks(range.size, num_tasks);

    auto responder = std::make_shared<common::Responder>(1);

    const auto chunk_bytesize = utils::random::number<size_t>(1, range.size);
    const auto config = std::make_shared<Config>(utils::random::number(1, 4), chunk_bytesize, utils::random::number<size_t>(1, chunk_bytesize), false /* do not force minimum chunk size */);

    std::vector<char> dst(size);
    auto dst_ptr = dst.data();

    // create tasks
    auto request = std::make_shared<Request>(range.start, utils::random::number(), num_tasks, size);

    size_t offset = start;

    Tasks tasks;
    for (unsigned i = 0; i < num_tasks; ++i)
    {
        // task offset is relative to the beginning of the request offset
        auto task = Task(request, offset, chunks[i]);
        offset += chunks[i];
        tasks.push_back(std::move(task));
    }

    Batch batch(path, params, std::move(range), dst_ptr, std::move(tasks), responder, config);

    std::atomic<bool> stopped(true);
    EXPECT_NO_THROW(batch.execute(stopped));

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);

    // verify data not read
    bool mismatch = false;
    for (size_t i = 0; i < size && !mismatch; ++i)
    {
        mismatch = dst[i] != static_cast<char>(data[start + i]);
    }
    EXPECT_TRUE(mismatch);
}

TEST(Read, Stopped_During_Read)
{
    unsigned num_requests = utils::random::number(1, 10);

    // File range to read
    const auto start = utils::random::number<size_t>(0, 1024);
    const auto size = utils::random::number<size_t>(512 * 1024, 1024 * 1024);
    EXPECT_LT(num_requests, size);

    auto range = Range(start, start + size);
    EXPECT_EQ(range.size, size);

    const auto data = utils::random::buffer(start + size);
    utils::temp::File file(data);
    const auto path = file.path;
    common::s3::S3ClientWrapper::Params params;

    // divide range into chunks - a chunk per request

    const auto chunks = utils::random::chunks(range.size, num_requests);

    auto responder = std::make_shared<common::Responder>(num_requests);

    const auto chunk_bytesize = utils::random::number<size_t>(1, range.size);
    const auto config = std::make_shared<Config>(utils::random::number(1, 4), chunk_bytesize, utils::random::number<size_t>(1, chunk_bytesize), false /* do not force minimum chunk size */);

    std::vector<char> dst(size);
    auto dst_ptr = dst.data();

    // create task for each request
    Tasks tasks;
    std::vector<std::shared_ptr<Request>> requests(num_requests);
    std::vector<size_t> offsets;
    auto offset = start;
    for (unsigned i = 0; i < num_requests; ++i)
    {
        requests[i] = std::make_shared<Request>(offset, i, 1, chunks[i]);
        EXPECT_EQ(requests[i]->bytesize, chunks[i]);
        EXPECT_EQ(requests[i]->offset, offset);

        // task offset is relative to the beginning of the request offset
        auto task = Task(requests[i], offset, chunks[i]);
        tasks.push_back(std::move(task));

        offsets.push_back(offset);
        offset += chunks[i];
    }

    Batch batch(path, params, std::move(range), dst_ptr, std::move(tasks), responder, config);

    std::atomic<bool> stopped(false);

    auto thread = utils::Thread([&]()
    {
        EXPECT_NO_THROW(batch.execute(stopped));
    });

    ::usleep(utils::random::number(300));
    stopped = true;

    // collect responses
    std::vector<common::Response> responses;
    std::set<unsigned> responded_requests;
    for (unsigned i = 0; i < num_requests; ++i)
    {
        auto r = batch.responder->pop();
        responded_requests.insert(r.index);
        responses.push_back(r);
    }

    EXPECT_EQ(responded_requests.size(), num_requests);

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);

    // verify that all responses were sent
    for (const auto & r : responses)
    {
        EXPECT_LT(r.index, num_requests);

        bool mismatch = false;
        const auto j_start = offsets[r.index]; // request offset is the file offset
        const auto j_end = j_start + chunks[r.index];
        for (size_t j = j_start; j < j_end; ++j)
        {
            char dst_ = dst[j - start];
            char data_ =  static_cast<char>(data[j]);
            mismatch = (data_ != dst_);
            if (mismatch)
            {
                break;
            }
        }

        if (r.ret == common::ResponseCode::Success)
        {
            // verify read data
            EXPECT_FALSE(mismatch);
        }
        else
        {
            EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
            // verify unread data
            EXPECT_TRUE(mismatch);
        }
    }
}

TEST(Read, Stopped_During_Async_Read)
{
    // mock S3
    utils::Dylib dylib("libstreamers3.so");
    auto mock_response_time = dylib.dlsym<void(*)(unsigned)>("runai_mock_s3_set_response_time_ms");
    auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");
    unsigned delay_ms = 1000;
    mock_response_time(delay_ms);
    auto guard = utils::ScopeGuard([&mock_cleanup](){
        mock_cleanup();
    });

    unsigned num_requests = utils::random::number(1, 10);

    // File range to read
    const auto start = utils::random::number<size_t>(0, 1024);
    const auto size = utils::random::number<size_t>(512 * 1024, 1024 * 1024);
    EXPECT_LT(num_requests, size);

    auto range = Range(start, start + size);
    EXPECT_EQ(range.size, size);

    const auto data = utils::random::buffer(start + size);
    std::string path("s3://" + utils::random::string() + "/" + utils::random::string());

    std::shared_ptr<common::s3::StorageUri> uri;
    EXPECT_NO_THROW(uri = std::make_shared<common::s3::StorageUri>(path));

    common::s3::Credentials credentials(
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr));

    common::s3::S3ClientWrapper::Params params(uri, credentials);

    // divide range into chunks - a chunk per request

    const auto chunks = utils::random::chunks(range.size, num_requests);

    auto responder = std::make_shared<common::Responder>(num_requests);

    const auto chunk_bytesize = utils::random::number<size_t>(1, range.size);
    const auto config = std::make_shared<Config>(utils::random::number(1, 4), chunk_bytesize, utils::random::number<size_t>(1, chunk_bytesize), false /* do not force minimum chunk size */);

    std::vector<char> dst(size);
    auto dst_ptr = dst.data();

    // create task for each request
    Tasks tasks;
    std::vector<std::shared_ptr<Request>> requests(num_requests);
    std::vector<size_t> offsets;
    auto offset = start;
    for (unsigned i = 0; i < num_requests; ++i)
    {
        requests[i] = std::make_shared<Request>(offset, i, 1, chunks[i]);
        EXPECT_EQ(requests[i]->bytesize, chunks[i]);
        EXPECT_EQ(requests[i]->offset, offset);

        // task offset is relative to the beginning of the request offset
        auto task = Task(requests[i], offset, chunks[i]);
        tasks.push_back(std::move(task));

        offsets.push_back(offset);
        offset += chunks[i];
    }

    Batch batch(path, params, std::move(range), dst_ptr, std::move(tasks), responder, config);

    std::atomic<bool> stopped(false);

    auto thread = utils::Thread([&]()
    {
        EXPECT_NO_THROW(batch.execute(stopped));
    });

    ::usleep(utils::random::number(300));

    common::s3::S3ClientWrapper::stop();
    stopped = true;

    // collect responses
    const auto start_time = std::chrono::steady_clock::now();
    std::vector<common::Response> responses;
    std::set<unsigned> responded_requests;
    for (unsigned i = 0; i < num_requests; ++i)
    {
        auto r = batch.responder->pop();
        responded_requests.insert(r.index);
        responses.push_back(r);
    }

    const auto end_time = std::chrono::steady_clock::now();
    const auto duration  = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    if (num_requests > 1)
    {
        // verify that not all the requests were executed
        EXPECT_LT(duration.count(), num_requests * delay_ms);
    }

    EXPECT_EQ(responded_requests.size(), num_requests);

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);

    unsigned count_terminated = 0;
    for (const auto & r : responses)
    {
        EXPECT_LT(r.index, num_requests);

        if (r.ret != common::ResponseCode::Success)
        {
            EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
            ++count_terminated;
        }
    }

    if (num_requests > 1)
    {
        EXPECT_GT(count_terminated, 0);
    }
}

}; // namespace runai::llm::streamer::impl
