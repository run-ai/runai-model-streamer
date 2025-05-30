#include "streamer/streamer.h"

#include <unistd.h>

#include <gtest/gtest.h>
#include <map>
#include <string>
#include <vector>
#include <chrono>

#include "common/response_code/response_code.h"

#include "utils/logging/logging.h"
#include "utils/random/random.h"
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
        _chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", utils::random::number<int>(1, 1024))
    {}

 protected:
    utils::temp::Env _size;
    utils::temp::Env _chunk_bytesize;
    utils::temp::Env _block_bytesize;
};

} // namespace

TEST_F(StreamerTest, Creation)
{
    void * streamer = nullptr;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));
    EXPECT_NE(streamer, nullptr);

    EXPECT_NO_THROW(runai_end(streamer));
}

TEST(Creation, Invalid_Parameter)
{
    void * streamer = nullptr;

    {
        utils::temp::Env size("RUNAI_STREAMER_CONCURRENCY", 0);
        utils::temp::Env chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", utils::random::number<int>(1, 2000));
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));
        EXPECT_EQ(streamer, nullptr);
    }

    {
        utils::temp::Env size("RUNAI_STREAMER_CONCURRENCY", utils::random::number<int>(1, 10));
        utils::temp::Env chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", 0);
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));
        EXPECT_EQ(streamer, nullptr);
    }

    {
        utils::temp::Env size("RUNAI_STREAMER_CONCURRENCY", utils::random::number<int>(1, 10));
        utils::temp::Env chunk_bytesize("RUNAI_STREAMER_CHUNK_BYTESIZE", 0);
        auto res = runai_start(&streamer);
        EXPECT_EQ(res, static_cast<int>(common::ResponseCode::InvalidParameterError));
        EXPECT_EQ(streamer, nullptr);
    }
}

TEST_F(StreamerTest, Read)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<unsigned char> v(size);
    res = runai_read(streamer, file.path.c_str(), 0, size, v.data());
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(v[i], expected[i]);
        if (v[i] != expected[i])
        {
            break;
        }
    }

    runai_end(streamer);
}

TEST_F(StreamerTest, Async)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    const auto expected = utils::Fd::read(file.path);
    EXPECT_EQ(expected.size(), size);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(runai_request(streamer, file.path.c_str(), 0, size, dst.data(), 1, sizes.data()), static_cast<int>(common::ResponseCode::Success));
    unsigned r = utils::random::number();
    EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::Success));
    EXPECT_EQ(r, 0);
    EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::FinishedError));
    EXPECT_EQ(r, 0);

    for (size_t i = 0; i < size; ++i)
    {
        EXPECT_EQ(dst[i], expected[i]);
        if (dst[i] != expected[i])
        {
            break;
        }
    }

    runai_end(streamer);
}

TEST_F(StreamerTest, Error)
{
    auto size = utils::random::number(100, 1000);
    const auto data = utils::random::buffer(utils::random::number(1, size-1));
    utils::temp::File file(data);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    const auto request_ret = runai_request(streamer, file.path.c_str(), 0, size, dst.data(), 1, sizes.data());
    if (request_ret == static_cast<int>(common::ResponseCode::EofError))
    {
        return;
    }

    EXPECT_EQ(request_ret, static_cast<int>(common::ResponseCode::Success));

    unsigned value = utils::random::number();
    unsigned r = value;
    EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::EofError));
    EXPECT_EQ(r, value);
    EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::FinishedError));
    EXPECT_EQ(r, value);

    runai_end(streamer);
}

TEST(Response, Description)
{
    auto __strings = std::map<int, std::string>
    {
        { static_cast<int>(common::ResponseCode::FileAccessError),                "File access error"                       },
        { static_cast<int>(common::ResponseCode::EofError),                       "End of file reached"                     },
        { static_cast<int>(common::ResponseCode::InvalidParameterError),          "Invalid request parameters"              },
        { static_cast<int>(common::ResponseCode::EmptyRequestError),              "Empty request parameters"                },
        { static_cast<int>(common::ResponseCode::BusyError),                      "Streamer is handling previous request"   },
        { static_cast<int>(common::ResponseCode::UnknownError),                   "Unknown Error"                           },
        { static_cast<int>(common::ResponseCode::FinishedError),                  "Finished all responses"                  },
        { static_cast<int>(common::ResponseCode::Success),                        "Request sent successfuly"                },
    };

    // errors

    for (auto response_code : {common::ResponseCode::FileAccessError, common::ResponseCode::EofError, common::ResponseCode::InvalidParameterError, common::ResponseCode::EmptyRequestError, common::ResponseCode::BusyError, common::ResponseCode::UnknownError, common::ResponseCode::FinishedError} )
    {
        std::string str = runai_response_str(static_cast<int>(response_code));

        const auto it = __strings.find(static_cast<int>(response_code));
        EXPECT_NE(it, __strings.end());

        EXPECT_EQ(str, it->second);
    }
}

TEST_F(StreamerTest, S3_Library_Not_Found)
{
    auto size = utils::random::number(100, 1000);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    const auto s3_path = "s3://" + utils::random::string() + "/" + utils::random::string();

    std::vector<char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);
    EXPECT_EQ(runai_request(streamer, s3_path.c_str(), 0, size, dst.data(), 1, sizes.data()), static_cast<int>(common::ResponseCode::Success));
    unsigned r = utils::random::number();
    EXPECT_EQ(runai_response(streamer, &r), static_cast<int>(common::ResponseCode::S3NotSupported));

    runai_end(streamer);
}

TEST_F(StreamerTest, End_Before_Read)
{
    auto size = utils::random::number(10000000, 100000000);
    const auto data = utils::random::buffer(size);
    utils::temp::File file(data);

    void * streamer;
    auto res = runai_start(&streamer);
    EXPECT_EQ(res, static_cast<int>(common::ResponseCode::Success));

    std::vector<unsigned char> dst(size);
    std::vector<size_t> sizes;
    sizes.push_back(size);

    EXPECT_EQ(runai_request(streamer, file.path.c_str(), 0, size, dst.data(), 1, sizes.data()), static_cast<int>(common::ResponseCode::Success));

    ::usleep(utils::random::number(400));

    const auto start_time = std::chrono::steady_clock::now();
    runai_end(streamer);
    const auto time_ = std::chrono::steady_clock::now();
    const auto duration  = std::chrono::duration_cast<std::chrono::milliseconds>(time_ - start_time);
    EXPECT_LT(duration.count(), 1000);
}

}; // namespace runai::llm::streamer
