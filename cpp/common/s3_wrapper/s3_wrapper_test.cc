#include "common/s3_wrapper/s3_wrapper.h"

#include <gtest/gtest.h>

#include "utils/dylib/dylib.h"
#include "utils/random/random.h"
#include "utils/logging/logging.h"
#include "utils/strings/strings.h"

namespace runai::llm::streamer::common::s3
{

struct S3WrappertTest : ::testing::Test
{
    S3WrappertTest() :
        uri("s3://" + utils::random::string() + "/" + utils::random::string())
    {}

    void TearDown() override
    {
        S3ClientWrapper::shutdown();
    }

    StorageUri uri;
};

TEST_F(S3WrappertTest, Creation_Sanity)
{
    S3ClientWrapper wrapper(uri);
}

TEST_F(S3WrappertTest, Creation)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    EXPECT_EQ(verify_mock(), 0);

    {
        S3ClientWrapper wrapper(uri);
        EXPECT_EQ(verify_mock(), 1);
    }
    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(S3WrappertTest, Read)
{
    S3ClientWrapper wrapper(uri);
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
        S3ClientWrapper wrapper(uri);
        std::vector<Range> ranges;
        wrapper.async_read(ranges, utils::random::number<size_t>(), nullptr);

        EXPECT_EQ(verify_mock(), 1);

        S3ClientWrapper::shutdown();
        EXPECT_EQ(verify_mock(), 1);
    }

    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(S3WrappertTest, List_Objects)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    EXPECT_EQ(verify_mock(), 0);

    {
        S3ClientWrapper wrapper(uri);
        EXPECT_EQ(verify_mock(), 1);

        char** object_keys = nullptr;
        size_t object_count = utils::random::number<size_t>();

        auto r = wrapper.list_objects(&object_keys, &object_count);
        EXPECT_EQ(r, common::ResponseCode::Success);

        if (object_count)
        {
            EXPECT_TRUE(object_keys != nullptr);
            for (size_t i = 0; i < object_count; ++i)
            {
                EXPECT_TRUE(object_keys[i] != nullptr);
                LOG(SPAM) << i << " " << object_keys[i];
            }
        }

        EXPECT_NO_THROW(utils::Strings::free_cstring_list(object_keys, object_count));
    }
    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(S3WrappertTest, Object_Bytesize)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    EXPECT_EQ(verify_mock(), 0);

    {
        S3ClientWrapper wrapper(uri);
        EXPECT_EQ(verify_mock(), 1);

        size_t object_bytesize = 0;

        auto r = wrapper.object_bytesize(&object_bytesize);
        EXPECT_EQ(r, common::ResponseCode::Success);
        EXPECT_GT(object_bytesize, 0);
    }
    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

TEST_F(S3WrappertTest, Sync_Read)
{
    utils::Dylib dylib("libstreamers3.so");
    auto verify_mock = dylib.dlsym<int(*)(void)>("runai_mock_s3_clients");
    EXPECT_EQ(verify_mock(), 0);

    const auto size = utils::random::number(100, 1000);
    auto offset = utils::random::number(1, size - 1);
    const auto data = utils::random::buffer(size);

    auto set_object_data = dylib.dlsym<void(*)(const char *, size_t)>("runai_mock_s3_set_object_data");
    set_object_data(reinterpret_cast<const char *>(data.data()), size);

    {
        S3ClientWrapper wrapper(uri);
        EXPECT_EQ(verify_mock(), 1);
        const auto to_read = size - offset;
        std::vector<char> buffer(to_read);

        auto r = wrapper.read(offset, to_read, buffer.data());
        EXPECT_EQ(r, common::ResponseCode::Success);

        for (size_t i = 0; i < to_read; ++i)
        {
            EXPECT_EQ(static_cast<char>(data[offset + i]), buffer[i]);
        }
    }
    S3ClientWrapper::shutdown();
    EXPECT_EQ(verify_mock(), 0);
}

}; // namespace runai::llm::streamer::common::s3
