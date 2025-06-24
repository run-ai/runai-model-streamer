#include "common/s3_wrapper/s3_wrapper.h"

#include <gtest/gtest.h>
#include <cstdint>

#include "utils/dylib/dylib.h"
#include "utils/random/random.h"
#include "utils/temp/env/env.h"
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
    {
        auto ptr = utils::random::number<uintptr_t>();
        request_id = reinterpret_cast<common::backend_api::ObjectRequestId_t>(ptr);
    }

    void TearDown() override
    {
        S3ClientWrapper::shutdown();
    }

    StorageUri uri;
    Credentials credentials;
    S3ClientWrapper::Params params;
    common::backend_api::ObjectRequestId_t request_id;
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
    Range range;
    auto response_code = wrapper.async_read(params, request_id, range, utils::random::number<size_t>(), nullptr);
    EXPECT_EQ(response_code, common::ResponseCode::Success);
}

TEST_F(S3WrappertTest, Response)
{
    S3ClientWrapper wrapper(params);
    Range range;
    auto response_code = wrapper.async_read(params, request_id, range, utils::random::number<size_t>(), nullptr);
    EXPECT_EQ(response_code, common::ResponseCode::Success);

    std::vector<backend_api::ObjectCompletionEvent_t> event_buffer;
    response_code = wrapper.async_read_response(event_buffer, 1);
    EXPECT_EQ(response_code, common::ResponseCode::Success);
    EXPECT_EQ(event_buffer.size(), 1);
    EXPECT_EQ(event_buffer.at(0).request_id, request_id);
    EXPECT_EQ(event_buffer.at(0).response_code, common::ResponseCode::Success);
}

TEST_F(S3WrappertTest, Cleanup)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    EXPECT_EQ(verify_mock(), 0);
    {
        S3ClientWrapper wrapper(params);
        Range range;
        wrapper.async_read(params, request_id, range, utils::random::number<size_t>(), nullptr);

        EXPECT_EQ(verify_mock(), 1);

        S3ClientWrapper::shutdown();
        EXPECT_EQ(verify_mock(), 1);
    }

    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(S3WrappertTest, Endpoint_Exists)
{
    auto endpoint = utils::random::string();
    utils::temp::Env endpoint_env("AWS_ENDPOINT_URL", endpoint);

    Credentials credentials_;
    S3ClientWrapper::Params params_(std::make_shared<StorageUri>(uri), credentials_);
    S3ClientWrapper wrapper(params);

    EXPECT_EQ(params_.config.endpoint_url, endpoint);
}

TEST_F(S3WrappertTest, Endpoint_In_Credentials)
{
    auto endpoint = utils::random::string();
    Credentials credentials_(nullptr, nullptr, nullptr, nullptr, endpoint.c_str());

    S3ClientWrapper::Params params_(std::make_shared<StorageUri>(uri), credentials_);
    S3ClientWrapper wrapper(params);

    EXPECT_EQ(params_.config.endpoint_url, endpoint);
}


}; // namespace runai::llm::streamer::common::s3
