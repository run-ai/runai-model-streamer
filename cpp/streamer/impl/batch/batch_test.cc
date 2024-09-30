#include "streamer/impl/batch/batch.h"

#include <gtest/gtest.h>
#include <utility>
#include <memory>

#include "utils/random/random.h"
#include "utils/temp/file/file.h"

#include "streamer/impl/file/file.h"

namespace runai::llm::streamer::impl
{

TEST(Batch, Finished_Until)
{
    // File range to read
    auto start = utils::random::number<size_t>(0, 1024);
    auto size = utils::random::number<size_t>(1, 1024 * 1024);

    // divide range into chunks - a chunk per task
    unsigned num_tasks = utils::random::number(1, 10);
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
    const auto path = utils::random::string();
    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_shared<common::s3::StorageUri>(path);
    }
    catch(const std::exception& e)
    {
    }

    Batch batch(path, uri, std::move(range), nullptr, std::move(tasks), responder, config);

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

    batch.finished_until(batch.range.end);

    EXPECT_EQ(batch.finished_until(), num_tasks);

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::Success);
}

TEST(Read, Sanity)
{
    // File range to read
    auto start = utils::random::number<size_t>(0, 1024);
    auto size = utils::random::number<size_t>(1, 1024 * 1024);
    auto range = Range(start, start + size);
    EXPECT_EQ(range.size, size);

    const auto data = utils::random::buffer(start + size);
    utils::temp::File file(data);
    const auto path = file.path;
    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_shared<common::s3::StorageUri>(path);
    }
    catch(const std::exception& e)
    {
    }

    // divide range into chunks - a chunk per task
    unsigned num_tasks = utils::random::number(1, 10);
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

    Batch batch(path, uri, std::move(range), dst_ptr, std::move(tasks), responder, config);

    EXPECT_NO_THROW(batch.execute());

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::Success);

    // verify read data
    for (size_t i = 0; i < size && dst[i] == data[i]; ++i)
    {
        EXPECT_EQ(dst[i], data[i]);
    }
}

TEST(Read, Error)
{
    std::string path;

    // File range to read
    auto start = utils::random::number<size_t>(0, 1024);
    auto size = utils::random::number<size_t>(2, 1024 * 1024);
    auto range = Range(start, start + size);
    EXPECT_EQ(range.size, size);

    const auto data = utils::random::buffer(start + size - utils::random::number<size_t>(1, size));
    utils::temp::File file(data);
    path = file.path;

    // divide range into chunks - a chunk per task
    unsigned num_tasks = utils::random::number(1, 10);
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

    std::shared_ptr<common::s3::StorageUri> uri;
    try
    {
        uri = std::make_shared<common::s3::StorageUri>(path);
    }
    catch(const std::exception& e)
    {
    }

    Batch batch(path, uri, std::move(range), dst_ptr, std::move(tasks), responder, config);

    EXPECT_NO_THROW(batch.execute());

    auto r = batch.responder->pop();
    EXPECT_EQ(r.ret, common::ResponseCode::EofError);
}

}; // namespace runai::llm::streamer::impl
