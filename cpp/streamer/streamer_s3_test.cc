#include "streamer/streamer.h"

#include <gtest/gtest.h>
#include <map>
#include <string>
#include <vector>

#include "common/response_code/response_code.h"

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/dylib/dylib.h"
#include "utils/fd/fd.h"
#include "utils/temp/env/env.h"
#include "utils/temp/file/file.h"

namespace runai::llm::streamer
{

namespace
{

struct StreamerTest : ::testing::Test
{
    StreamerTest() :
        _size("RUNAI_STREAMER_CONCURRENCY", utils::random::number<int>(1, 10)),
        _chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", utils::random::number<int>(1, 1024)),
        s3_path("s3://" + utils::random::string() + "/" + utils::random::string())
    {}

    ~StreamerTest()
    {
        utils::Dylib dylib("libstreamers3.so");
        auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");
        mock_cleanup();
    }

 protected:
    utils::temp::Env _size;
    utils::temp::Env _chunk_bytesize;
    utils::temp::Env _block_bytesize;
    std::string s3_path;
};

} // namespace


TEST_F(StreamerTest, Sync_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<char> v(size);
    res = runai_read(streamer, s3_path.c_str(), 0, size, v.data());
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Async_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    auto total_size = utils::random::number(100, 10000);
    const auto num_chunks = utils::random::number(1, 10);
    EXPECT_LT(num_chunks, total_size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    const auto chunks = utils::random::chunks(total_size, num_chunks);

    std::vector<char> dst(total_size);
    std::vector<size_t> sizes;

    const auto offset = utils::random::number<size_t>();

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        sizes.push_back(chunks[i]);
    }

    EXPECT_EQ(runai_request(streamer, s3_path.c_str(), offset, total_size, dst.data(), sizes.size(), sizes.data()), static_cast<int>(common::ResponseCode::Success));

    std::set<unsigned> expected;
    unsigned r;
    for (unsigned i = 0; i < num_chunks; ++i)
    {
        r = utils::random::number();
        EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::Success));
        EXPECT_GE(r, 0);
        EXPECT_LT(r, num_chunks);
        EXPECT_EQ(expected.count(r), 0);
        expected.insert(r);
    }

    EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::FinishedError));

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Error)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    auto mock_cleanup = dylib.dlsym<void(*)()>("runai_mock_s3_cleanup");

    for (const auto response_code : { common::ResponseCode::FileAccessError, common::ResponseCode::InvalidParameterError })
    {
        EXPECT_EQ(verify_mock(), 0);
        utils::temp::Env env_rc("RUNAI_STREAMER_S3_MOCK_RESPONSE_CODE", static_cast<int>(response_code));
        auto size = utils::random::number(100, 1000);
        const auto data = utils::random::buffer(size);

        void * streamer;
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

        std::vector<char> v(size);
        res = runai_read(streamer, s3_path.c_str(), 0, size, v.data());
        EXPECT_EQ(res, static_cast<int>(response_code));

        runai_end(streamer);
        EXPECT_EQ(verify_mock(), 0);
        mock_cleanup();
    }
}

TEST_F(StreamerTest, Read_Object_To_File)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    const auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);

    auto set_object_data = dylib.dlsym<void(*)(const char *, size_t)>("runai_mock_s3_set_object_data");
    set_object_data(reinterpret_cast<const char *>(data.data()), size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // create destination path
    const auto prev = utils::random::buffer(size + utils::random::number(1, 100));
    utils::temp::File dest_file(prev);

    res = runai_read_object_to_file(streamer, s3_path.c_str(), dest_file.path.c_str());
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // verify reading
    // verify read data
    auto result = utils::Fd::read(dest_file.path);

    bool mismatch = false;
    for (size_t i = 0; i < size && !mismatch; ++i)
    {
        mismatch = result[i] != data[i];
    }
    EXPECT_FALSE(mismatch);

    EXPECT_THROW(utils::Fd::read(dest_file.path, size + 1, 1), std::exception);

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Read_Object_With_Wrong_Size)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    const auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // create destination path
    utils::temp::Path dest_path;

    res = runai_read_object_to_file(streamer, s3_path.c_str(), dest_path.path.c_str());
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::FileAccessError));

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Read_Object_With_Wrong_Path)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    const auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // create destination path
    utils::temp::Path dest_path;

    const auto bad_path = utils::random::string();

    res = runai_read_object_to_file(streamer, bad_path.c_str(), dest_path.path.c_str());
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Read_Object_Do_Not_Support_S3_Destination)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    const auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    // create destination path
    const std::string s3_dest_path("s3://" + utils::random::string() + "/" + utils::random::string());

    res = runai_read_object_to_file(streamer, s3_path.c_str(), s3_dest_path.c_str());
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, Stop_Before_Async_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    auto stop_mock = dylib.dlsym<void(*)(void)>("runai_stop_s3_clients");

    auto total_size = utils::random::number(100, 10000);
    const auto num_chunks = utils::random::number(1, 10);
    EXPECT_LT(num_chunks, total_size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    const auto chunks = utils::random::chunks(total_size, num_chunks);

    std::vector<char> dst(total_size);
    std::vector<size_t> sizes;

    const auto offset = utils::random::number<size_t>();

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        sizes.push_back(chunks[i]);
    }

    stop_mock();
    EXPECT_EQ(runai_request(streamer, s3_path.c_str(), offset, total_size, dst.data(), sizes.size(), sizes.data()), static_cast<int>(common::ResponseCode::Success));

    // request was not sent to the S3 server
    unsigned r;
    EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::FinishedError));

    runai_end(streamer);
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(StreamerTest, End_During_Async_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");

    auto mock_response_time = dylib.dlsym<void(*)(unsigned)>("runai_mock_s3_set_response_time_ms");
    unsigned delay_ms = 1000;
    mock_response_time(delay_ms);

    auto total_size = utils::random::number(100, 10000);
    const auto num_chunks = utils::random::number(1, 10);
    EXPECT_LT(num_chunks, total_size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    const auto chunks = utils::random::chunks(total_size, num_chunks);

    std::vector<char> dst(total_size);
    std::vector<size_t> sizes;

    const auto offset = utils::random::number<size_t>();

    for (unsigned i = 0; i < num_chunks; ++i)
    {
        sizes.push_back(chunks[i]);
    }

    EXPECT_EQ(runai_request(streamer, s3_path.c_str(), offset, total_size, dst.data(), sizes.size(), sizes.data()), static_cast<int>(common::ResponseCode::Success));

    ::usleep(utils::random::number(300));

    runai_end(streamer);

    EXPECT_EQ(verify_mock(), 0);
}

}; // namespace runai::llm::streamer
