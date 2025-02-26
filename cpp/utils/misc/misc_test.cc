#include "utils/misc/misc.h"

#include <gtest/gtest.h>

namespace runai::llm::streamer::utils::misc
{

TEST(GetFdLimit, Sanity)
{
    EXPECT_NO_THROW(get_max_file_descriptors());

    auto res = get_max_file_descriptors();
    EXPECT_NE(res, 0);
}

TEST(GetGlibVersion, Sanity)
{
    EXPECT_NO_THROW(get_glibc_version());
}

} // namespace runai::llm::streamer::utils::misc
