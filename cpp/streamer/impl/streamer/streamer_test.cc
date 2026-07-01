#include "streamer/impl/streamer/streamer.h"

#include <gtest/gtest.h>
#include <atomic>
#include <string>
#include <utility>
#include <vector>
#include <set>

#include "common/exception/exception.h"

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
    Config config(utils::random::number(2, 30), utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    std::vector<unsigned char> v(size);
    auto result = streamer.sync_read(file.path, 0, size, v.data(), credentials);
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
    Config config(utils::random::number(2, 30), utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);
    common::s3::Credentials credentials(
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
        (utils::random::boolean() ? utils::random::string().c_str() : nullptr));

    std::vector<char> v(size);
    auto r = streamer.sync_read(utils::random::string(), 0, size, v.data(), credentials);
    EXPECT_EQ(r, common::ResponseCode::FileAccessError);
}

TEST(Sync, End_Of_File_Error)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size / 2);
    utils::temp::File file(data);

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(2, 30), utils::random::number(2, 30), chunk_size, bulk_size, false /* do not enforce minimum */);
    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> v(size);

    for (size_t file_offset : {0UL, utils::random::number<size_t>(size, 100 * size)})
    {
        auto r = streamer.sync_read(file.path, file_offset, size, v.data(), credentials);
        EXPECT_EQ(r, common::ResponseCode::EofError);
    }

    for (size_t file_offset : {utils::random::number<size_t>(size/2, size), utils::random::number<size_t>(size, 100 * size)})
    {
        auto r = streamer.sync_read(file.path, file_offset, utils::random::number<size_t>(1, size/2), v.data(), credentials);
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
        Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
        Streamer streamer(config);

        auto r = streamer.sync_read(file.path, offset_start, size_to_read, v.data(), credentials);
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
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst.data(), 1, sizes.data(), credentials), common::ResponseCode::Success);
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
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    Streamer streamer(config);

    common::s3::Credentials credentials;

    std::vector<unsigned char> dst(size);
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst.data(), num_chunks, chunks.data(), credentials), common::ResponseCode::Success);

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
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);
    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> dst(size);
    EXPECT_EQ(streamer.async_read(utils::random::string(), 0, size, dst.data(), num_chunks, chunks.data(), credentials), common::ResponseCode::Success);

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

    auto request_ret = streamer.async_read(file.path, 0, size, dst.data(), num_chunks, chunks.data(), credentials);

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
    Config config(utils::random::number(1, 10), utils::random::number(1, 10), chunk_size, bulk_size, false /* do not enforce minimum */);

    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> dst(size);
    // sending zero instead of num_chunks
    EXPECT_EQ(streamer.async_read(utils::random::string(), 0, size, dst.data(), 0, chunks.data(), credentials), common::ResponseCode::InvalidParameterError);

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
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<char> dst(size);
    // sending zero instead of num_chunks

    for (unsigned num_chunks_ : {0U, num_chunks})
    {
        auto result = streamer.async_read(utils::random::string(), 0, 0, dst.data(), num_chunks_, chunks.data(), credentials);
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
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);

    // first request succeeds
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst.data(), 1, sizes.data(), credentials), common::ResponseCode::Success);

    // second request fails
    EXPECT_EQ(streamer.async_read(file.path, 0, size, dst.data(), 1, sizes.data(), credentials), common::ResponseCode::BusyError);

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

TEST(AsyncRequest, InvalidScheme)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    std::string s3_path = "s3://s3-bucket/file-01.txt";
    std::string gcs_path = "gs://gcs-bucket/file-02.txt";

    const auto chunk_size = utils::random::number<size_t>(1, 1024);
    const auto bulk_size = utils::random::number<size_t>(1, chunk_size);
    Config config(utils::random::number(1, 20), utils::random::number(1, 20), chunk_size, bulk_size, false /* do not enforce minimum */);

    common::s3::Credentials credentials;

    Streamer streamer(config);

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);

    std::vector<std::string> paths = {s3_path, gcs_path};
    std::vector<size_t> file_offsets = {0};
    std::vector<size_t> bytesizes = {size};
    std::vector<void *> dsts = {dst.data()};
    std::vector<unsigned> num_sizes = {1};
    std::vector<std::vector<size_t>> internal_sizes =  { sizes };

    EXPECT_THROW(streamer.async_request(paths, file_offsets, bytesizes, dsts, num_sizes, internal_sizes, credentials), runai::llm::streamer::common::Exception);
}

namespace
{

std::set<std::string> paths_of(const std::vector<std::pair<std::string, size_t>> & entries)
{
    std::set<std::string> result;
    for (const auto & entry : entries)
    {
        result.insert(entry.first);
    }
    return result;
}

} // namespace

TEST(ListFiles, FilesystemBasicListingAndSizes)
{
    Streamer streamer;
    common::s3::Credentials credentials;

    utils::temp::Dir dir;
    const auto data_a = utils::random::buffer(utils::random::number(1, 1000));
    const auto data_b = utils::random::buffer(utils::random::number(1, 1000));
    utils::temp::File a(dir.path, "a.bin", data_a);
    utils::temp::File b(dir.path, "b.bin", data_b);

    const auto entries = streamer.list_files(dir.path, true, {}, {}, credentials);

    EXPECT_EQ(entries.size(), 2u);
    bool found_a = false;
    bool found_b = false;
    for (const auto & entry : entries)
    {
        if (entry.first == a.path) { EXPECT_EQ(entry.second, data_a.size()); found_a = true; }
        if (entry.first == b.path) { EXPECT_EQ(entry.second, data_b.size()); found_b = true; }
    }
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
}

TEST(ListFiles, FilesystemRecursive)
{
    Streamer streamer;
    common::s3::Credentials credentials;

    utils::temp::Dir dir;
    utils::temp::File root_file(dir.path, "root.bin", utils::random::buffer(10));
    utils::temp::Dir sub(dir.path, "subdir");
    utils::temp::File nested(sub.path, "nested.bin", utils::random::buffer(10));

    const auto recursive = paths_of(streamer.list_files(dir.path, true, {}, {}, credentials));
    const auto non_recursive = paths_of(streamer.list_files(dir.path, false, {}, {}, credentials));

    EXPECT_TRUE(recursive.count(root_file.path));
    EXPECT_TRUE(recursive.count(nested.path));

    EXPECT_TRUE(non_recursive.count(root_file.path));
    EXPECT_FALSE(non_recursive.count(nested.path));
}

TEST(ListFiles, FilesystemAllowPattern)
{
    Streamer streamer;
    common::s3::Credentials credentials;

    utils::temp::Dir dir;
    utils::temp::File st(dir.path, "model.safetensors", utils::random::buffer(10));
    utils::temp::File js(dir.path, "config.json", utils::random::buffer(10));

    const auto paths = paths_of(streamer.list_files(dir.path, true, {"*.safetensors"}, {}, credentials));

    EXPECT_TRUE(paths.count(st.path));
    EXPECT_FALSE(paths.count(js.path));
}

TEST(ListFiles, FilesystemIgnorePattern)
{
    Streamer streamer;
    common::s3::Credentials credentials;

    utils::temp::Dir dir;
    utils::temp::File st(dir.path, "model.safetensors", utils::random::buffer(10));
    utils::temp::File js(dir.path, "config.json", utils::random::buffer(10));

    const auto paths = paths_of(streamer.list_files(dir.path, true, {}, {"*.json"}, credentials));

    EXPECT_TRUE(paths.count(st.path));
    EXPECT_FALSE(paths.count(js.path));
}

TEST(ListFiles, FilesystemNonExistentPathThrows)
{
    Streamer streamer;
    common::s3::Credentials credentials;

    const std::string missing = "./" + utils::random::string() + "/" + utils::random::string();
    try
    {
        streamer.list_files(missing, true, {}, {}, credentials);
        FAIL() << "expected an exception for a non-existent path";
    }
    catch (const common::Exception & e)
    {
        EXPECT_EQ(e.error(), common::ResponseCode::FileAccessError);
    }
}

TEST(ListFiles, FilesystemEmptyDirectory)
{
    Streamer streamer;
    common::s3::Credentials credentials;

    utils::temp::Dir dir;

    const auto entries = streamer.list_files(dir.path, true, {}, {}, credentials);
    EXPECT_TRUE(entries.empty());
}

}; // namespace runai::llm::streamer::impl
