#include "streamer/impl/streamer/streamer.h"

#include <gtest/gtest.h>
#include <atomic>
#include <vector>
#include <set>

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/fd/fd.h"
#include "utils/temp/file/file.h"

namespace runai::llm::streamer::impl
{

TEST(Creation, Default)
{
    Config config;
    Streamer streamer(config);
    auto r = streamer.response();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
}

TEST(Creation, Sanity)
{
    Streamer streamer;
    auto r = streamer.response();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
}

TEST(Sync, Sanity)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    common::s3::Credentials credentials(
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr));

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    std::vector<unsigned char> v(size);
    auto result = streamer.request(file.path, 0, size, v.data(), credentials);
    EXPECT_EQ(result, common::ResponseCode::Success);

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(v[i], expected[i]);
        if (v[i] != expected[i])
        {
            break;
        }
    }
}

TEST(Sync, File_Not_Found_Error)
{
    auto size = utils::random::number(100, 1000);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);
    common::s3::Credentials credentials(
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr));

    std::vector<char> v(size);
    auto r = streamer.request(utils::random::string(), 0, size, v.data(), credentials);
    EXPECT_EQ(r, common::ResponseCode::FileAccessError);
}

TEST(Sync, End_Of_File_Error)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size / 2);
    utils::temp::File file(data);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> v(size);

    for (size_t file_offset : {0UL, utils::random::number<size_t>(size, 100 * size)})
    {
        auto r = streamer.request(file.path, file_offset, size, v.data(), credentials);
        EXPECT_EQ(r, common::ResponseCode::EofError);
    }

    for (size_t file_offset : {utils::random::number<size_t>(size/2, size), utils::random::number<size_t>(size, 100 * size)})
    {
        auto r = streamer.request(file.path, file_offset, utils::random::number<size_t>(1, size/2), v.data(), credentials);
        EXPECT_EQ(r, common::ResponseCode::EofError);
    }
}

TEST(Sync, Offset)
{
    auto size = 1024;
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    auto offset_end = utils::random::number<size_t>(2, size);
    auto offset_start = utils::random::number<size_t>(offset_end - 1);
    auto size_to_read = offset_end - offset_start;

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);

    common::s3::Credentials credentials;

    std::vector<unsigned char> v(size_to_read);
    {
        Config config(utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
        Streamer streamer(config);

        auto r = streamer.request(file.path, offset_start, size_to_read, v.data(), credentials);
        EXPECT_EQ(r, common::ResponseCode::Success);
    }

    for (size_t i = 0; i < size_to_read; ++i)
    {
        EXPECT_EQ(v[i], expected[i + offset_start]);
        if (v[i] != expected[i + offset_start])
        {
            break;
        }
    }
}

TEST(Async, Sanity)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(streamer.request(file.path, 0, size, dst.data(), 1, sizes.data(), credentials), common::ResponseCode::Success);
    auto r = streamer.response();
    EXPECT_EQ(r.ret, common::ResponseCode::Success);
    EXPECT_EQ(r.index, 0);
    r = streamer.response();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst[i], expected[i]);
        if (dst[i] != expected[i])
        {
            break;
        }
    }
}

TEST(Async, Requests)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);
    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    common::s3::Credentials credentials;

    std::vector<unsigned char> dst(size);
    EXPECT_EQ(streamer.request(file.path, 0, size, dst.data(), num_chunks, chunks.data(), credentials), common::ResponseCode::Success);

    // wait for all the requests to finish
    std::set<int> expected_responses;

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        expected_responses.insert(i);
    }

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        const auto r = streamer.response();
        EXPECT_EQ(r.ret, common::ResponseCode::Success);
        LOG(SPAM) << "received response of request " << r.index;
        EXPECT_EQ(expected_responses.count(r.index), 1);
        expected_responses.erase(r.index);
    }

    EXPECT_TRUE(expected_responses.empty());
    auto r = streamer.response();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst[i], expected[i]);
        if (dst[i] != expected[i])
        {
            break;
        }
    }
}

TEST(Async, File_Not_Found_Error)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);
    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> dst(size);
    EXPECT_EQ(streamer.request(utils::random::string(), 0, size, dst.data(), num_chunks, chunks.data(), credentials), common::ResponseCode::Success);

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        const auto r = streamer.response();
        EXPECT_EQ(r.ret, common::ResponseCode::FileAccessError);
    }
}

TEST(Async, End_Of_File_Error)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    auto chunks = utils::random::chunks(size, num_chunks);

    // write data just for the first chunks

    const auto chunk_size = utils::random::number<size_t>(10, size - 1);
    const auto block_size = utils::random::number<size_t>(1, chunk_size);

    LOG(DEBUG) << "writing only " << chunk_size << " bytes";
    const auto data = utils::random::buffer(chunk_size);
    utils::temp::File file(data);

    Config config(utils::random::number(1, 20), chunk_size, block_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    std::vector<char> dst(size);

    common::s3::Credentials credentials;

    auto request_ret = streamer.request(file.path, 0, size, dst.data(), num_chunks, chunks.data(), credentials);

    EXPECT_EQ(request_ret, common::ResponseCode::Success);

    // wait for all the requests to finish

    unsigned count_successful = 0;
    for (unsigned i = 0; i < num_chunks; ++i)
    {
        const auto r = streamer.response();
        LOG(SPAM) << "received response of request " << r.index << " : " << r.ret;
        if (r.ret == common::ResponseCode::Success)
        {
            ++count_successful;
        }
        else
        {
            EXPECT_EQ(r.ret, common::ResponseCode::EofError);
        }
    }
    EXPECT_LT(count_successful, num_chunks);

    auto r = streamer.response();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
}

TEST(Async, Zero_Requests_Error)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 10), chunk_size, bulk_size, false /* do not enforce minimum */);

    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> dst(size);
    // sending zero instead of num_chunks
    EXPECT_EQ(streamer.request(utils::random::string(), 0, size, dst.data(), 0, chunks.data(), credentials), common::ResponseCode::InvalidParameterError);

    // wait for all the requests to finish

    auto r = streamer.response();
    EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
}

TEST(Async, Zero_Bytes_To_Read_Error)
{
    auto size = utils::random::number(100, 1000);

    // create internal division
    const unsigned num_chunks = utils::random::number(1, 20);
    EXPECT_LT(num_chunks, size);

    auto chunks = utils::random::chunks(size, num_chunks);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> dst(size);
    // sending zero instead of num_chunks

    for (unsigned num_chunks_ : {0U, num_chunks})
    {
        auto result = streamer.request(utils::random::string(), 0, 0, dst.data(), num_chunks_, chunks.data(), credentials);
        if (num_chunks_ > 0)
        {
            EXPECT_EQ(result, common::ResponseCode::InvalidParameterError);
        }
        else
        {
            EXPECT_EQ(result, common::ResponseCode::EmptyRequestError);
        }

        // wait for all the requests to finish

        auto r = streamer.response();
        EXPECT_EQ(r.ret, common::ResponseCode::FinishedError);
    }
}

TEST(Async, Busy_Error)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);

    // first request succeeds
    EXPECT_EQ(streamer.request(file.path, 0, size, dst.data(), 1, sizes.data(), credentials), common::ResponseCode::Success);

    // second request fails
    EXPECT_EQ(streamer.request(file.path, 0, size, dst.data(), 1, sizes.data(), credentials), common::ResponseCode::BusyError);

    // read response of the first request
    EXPECT_EQ(streamer.response().ret, common::ResponseCode::Success);
    EXPECT_EQ(streamer.response().ret, common::ResponseCode::FinishedError);

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst[i], expected[i]);
        if (dst[i] != expected[i])
        {
            break;
        }
    }
}

}; // namespace runai::llm::streamer::impl
