#include "s3/client_configuration/client_configuration.h"

#include <gtest/gtest.h>

#include "s3/s3_init/s3_init.h"
#include "utils/temp/env/env.h"

namespace runai::llm::streamer::impl::s3
{

namespace
{

constexpr char max_retries_env[] = "RUNAI_STREAMER_S3_MAX_RETRIES";
using RetryStrategyType = Aws::S3Crt::S3CrtClientConfiguration::CrtRetryStrategyConfig::CrtRetryStrategyType;

} // namespace

TEST(S3RetryConfiguration, KeepsCrtDefaultWhenUnset)
{
    utils::temp::UnsetEnv restore(max_retries_env);
    S3Init init;

    ClientConfiguration configuration;

    EXPECT_EQ(configuration.config.crtRetryStrategyConfig.crtRetryStrategyType,
              RetryStrategyType::NOT_SET);
}

TEST(S3RetryConfiguration, ConfiguresExplicitRetryLimit)
{
    utils::temp::UnsetEnv restore(max_retries_env);
    utils::temp::Env max_retries(max_retries_env, 4UL);
    S3Init init;

    ClientConfiguration configuration;

    EXPECT_EQ(configuration.config.crtRetryStrategyConfig.crtRetryStrategyType,
              RetryStrategyType::EXPONENTIAL_BACKOFF);
    EXPECT_EQ(configuration.config.crtRetryStrategyConfig.config.maxRetries, 4U);
}

TEST(S3RetryConfiguration, ZeroAllowsInitialAttemptWithoutNativeRetries)
{
    utils::temp::UnsetEnv restore(max_retries_env);
    utils::temp::Env max_retries(max_retries_env, 0UL);
    S3Init init;

    ClientConfiguration configuration;

    EXPECT_EQ(configuration.config.crtRetryStrategyConfig.crtRetryStrategyType,
              RetryStrategyType::EXPONENTIAL_BACKOFF);
    EXPECT_EQ(configuration.config.crtRetryStrategyConfig.config.maxRetries, 0U);
}

} // namespace runai::llm::streamer::impl::s3
