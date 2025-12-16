#include "utils/semver/semver.h"

#include <gtest/gtest.h>

#include "utils/random/random.h"

namespace runai::llm::streamer::utils
{

TEST(GetGlibVersion, Sanity)
{
    EXPECT_NO_THROW(get_glibc_version());
}

} // namespace runai::llm::streamer::utils
