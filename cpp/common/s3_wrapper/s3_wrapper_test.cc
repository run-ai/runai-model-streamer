#include "common/s3_wrapper/s3_wrapper.h"

#include <gtest/gtest.h>

#include "utils/dylib/dylib.h"
#include "utils/random/random.h"

namespace runai::llm::streamer::common::s3
{

struct S3WrappertTest : ::testing::Test
{
    S3WrappertTest() :
        uri("s3://" + utils::random::string() + "/" + utils::random::string()),
        credentials(
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr),
            (utils::random::boolean() ? utils::random::string().c_str() : nullptr)),
        params(std::make_shared<StorageUri>(uri), credentials)
    {}

    void TearDown() override
    {
        S3ClientWrapper::shutdown();
    }

    StorageUri uri;
    Credentials credentials;
    S3ClientWrapper::Params params;
};

TEST_F(S3WrappertTest, Creation_Sanity)
{
    EXPECT_NO_THROW(S3ClientWrapper wrapper(params));
}

TEST_F(S3WrappertTest, Creation)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    EXPECT_EQ(verify_mock(), 0);

    {
        S3ClientWrapper wrapper(params);
        EXPECT_EQ(verify_mock(), 1);
    }
    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(S3WrappertTest, Read)
{
    S3ClientWrapper wrapper(params);
    std::vector<Range> ranges;
    auto response_code = wrapper.async_read(ranges, utils::random::number<size_t>(), nullptr);
    EXPECT_EQ(response_code, common::ResponseCode::Success);
}

TEST_F(S3WrappertTest, Cleanup)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    EXPECT_EQ(verify_mock(), 0);
    {
        S3ClientWrapper wrapper(params);
        std::vector<Range> ranges;
        wrapper.async_read(ranges, utils::random::number<size_t>(), nullptr);

        EXPECT_EQ(verify_mock(), 1);

        S3ClientWrapper::shutdown();
        EXPECT_EQ(verify_mock(), 1);
    }

    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

}; // namespace runai::llm::streamer::common::s3
