#include "utils/strings/strings.h"
#include <gtest/gtest.h>

#include "utils/random/random.h"

namespace runai::llm::streamer::utils
{

TEST(Strings, Sanity)
{
    for (size_t expected : {0UL, utils::random::number<size_t>(10)})
    {
        std::vector<std::string> v;
        for (size_t i = 0; i < expected; ++i)
        {
            v.push_back(utils::random::string(utils::random::number(1, 100)));
        }

        char** list = nullptr;
        size_t size = utils::random::number<size_t>();

        EXPECT_NO_THROW(utils::Strings::create_cstring_list(v, &list, &size));

        EXPECT_EQ(size, expected);

        for (size_t i = 0; i < expected; ++i)
        {
            std::string str(list[i]);
            EXPECT_EQ(v[i], str);
        }

        EXPECT_NO_THROW(utils::Strings::free_cstring_list(list, size));
    }
}

} // namespace runai::llm::streamer::utils
