#include "s3/client_configuration/client_configuration.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "s3/s3_init/s3_init.h"
#include "utils/temp/env/env.h"

namespace runai::llm::streamer::impl::s3
{

// The SDK must be initialized before a client configuration can be built, and IMDS must be off or the
// constructor spends five seconds asking EC2 for a region (see the note in s3.cc).
//
// RUNAI_STREAMER_S3_TARGET_GBPS is unset here rather than per test: every case measures against the
// SDK's own default, so a value inherited from the environment would change what is being compared.
class ClientConfigurationTest : public ::testing::Test
{
 protected:
    void SetUp() override
    {
        _imds = std::make_unique<utils::temp::Env>(std::string("AWS_EC2_METADATA_DISABLED"), std::string("true"));
        _target = std::make_unique<utils::temp::UnsetEnv>(std::string("RUNAI_STREAMER_S3_TARGET_GBPS"));
        _init = std::make_unique<S3Init>();
    }

    void TearDown() override
    {
        _init.reset();
        _target.reset();
        _imds.reset();
    }

    // What the SDK targets for one client, read rather than hardcoded: the scaling below is relative to
    // it, so a change in the SDK default must not fail this suite.
    double per_reader() const
    {
        return ClientConfiguration(1).config.throughputTargetGbps;
    }

    std::unique_ptr<utils::temp::Env> _imds;
    std::unique_ptr<utils::temp::UnsetEnv> _target;
    std::unique_ptr<S3Init> _init;
};

// One client carries the whole capacity, so its target is the per-reader figure times the readers.
TEST_F(ClientConfigurationTest, Target_Scales_With_The_Reader_Count)
{
    EXPECT_DOUBLE_EQ(ClientConfiguration(8).config.throughputTargetGbps, per_reader() * 8);
    EXPECT_GT(per_reader(), 0.0) << "a zero per-reader target would make the scaling vacuous";
}

// Unstated must not scale the target down: it is the client the SDK would have built anyway.
TEST_F(ClientConfigurationTest, An_Unstated_Reader_Count_Leaves_The_Default)
{
    EXPECT_DOUBLE_EQ(ClientConfiguration(0).config.throughputTargetGbps, per_reader());
    EXPECT_DOUBLE_EQ(ClientConfiguration().config.throughputTargetGbps, per_reader());
}

// One of our ranged reads is one CRT part. Left to its own 8 MiB default the CRT would split a larger
// read into parts underneath us, on top of the connections this client already pools.
TEST_F(ClientConfigurationTest, The_Part_Size_Follows_The_Read_Size)
{
    EXPECT_EQ(ClientConfiguration(8, 32 * 1024 * 1024).config.partSize, 32u * 1024 * 1024);
}

// Unstated leaves the SDK default, so a caller that says nothing gets the client it always got.
TEST_F(ClientConfigurationTest, An_Unstated_Part_Size_Leaves_The_Default)
{
    const auto sdk_default = ClientConfiguration().config.partSize;

    EXPECT_GT(sdk_default, 0u);
    EXPECT_EQ(ClientConfiguration(8, 0).config.partSize, sdk_default);
}

// The variable has always named the target for ONE client, so it scales like the default it replaces.
// Taking it literally would cut an existing user's total by the reader count, because they used to get
// one client at this target per reader.
TEST_F(ClientConfigurationTest, The_Override_Is_Per_Reader_Too)
{
    utils::temp::Env target(std::string("RUNAI_STREAMER_S3_TARGET_GBPS"), 25UL);

    EXPECT_DOUBLE_EQ(ClientConfiguration(8).config.throughputTargetGbps, 200.0);
    EXPECT_DOUBLE_EQ(ClientConfiguration(1).config.throughputTargetGbps, 25.0);
}

}; // namespace runai::llm::streamer::impl::s3
