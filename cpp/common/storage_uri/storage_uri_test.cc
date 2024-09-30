#include "common/storage_uri/storage_uri.h"

#include <gtest/gtest.h>

#include <memory>

#include "utils/random/random.h"
#include "utils/temp/env/env.h"

namespace runai::llm::streamer::common::s3
{

TEST(Uri, Empty)
{
    std::unique_ptr<StorageUri> uri;
    EXPECT_THROW(uri = std::make_unique<StorageUri>(""), std::exception);
}

TEST(Uri, Valid_S3_Path)
{
    auto bucket = utils::random::string();
    auto path = utils::random::string();
    auto s3_path = "s3://" + bucket + "/" + path;
    std::unique_ptr<StorageUri> uri;
    EXPECT_NO_THROW(uri = std::make_unique<StorageUri>(s3_path));
    EXPECT_EQ(uri->bucket, bucket);
    EXPECT_EQ(uri->path, path);
    EXPECT_TRUE(uri->endpoint.empty());
}

TEST(Valid, Empty_Path)
{
    auto bucket = utils::random::string();
    auto s3_path = "s3://" + bucket + "/";
    std::unique_ptr<StorageUri> uri;
    EXPECT_THROW(uri = std::make_unique<StorageUri>(s3_path), std::exception);
}

TEST(Valid, Empty_S3_Path)
{
    std::unique_ptr<StorageUri> uri;
    EXPECT_THROW(uri = std::make_unique<StorageUri>("s3://"), std::exception);
}

TEST(Valid, Empty_Bucket)
{
    auto path = utils::random::string();
    auto s3_path = "s3:///" + path;
    std::unique_ptr<StorageUri> uri;
    EXPECT_THROW(uri = std::make_unique<StorageUri>(s3_path), std::exception);
}

TEST(Endpoint, Exists)
{
    auto bucket = utils::random::string();
    auto path = utils::random::string();
    auto s3_path = "s3://" + bucket + "/" + path;
    std::unique_ptr<StorageUri> uri;

    auto endpoint = utils::random::string();
    utils::temp::Env size("RUNAI_STREAMER_S3_ENDPOINT", endpoint);

    EXPECT_NO_THROW(uri = std::make_unique<StorageUri>(s3_path));
    EXPECT_EQ(uri->bucket, bucket);
    EXPECT_EQ(uri->path, path);
    EXPECT_EQ(uri->endpoint, endpoint);
}

}; // namespace runai::llm::streamer::common::s3
