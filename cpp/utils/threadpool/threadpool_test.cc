#include "utils/threadpool/threadpool.h"

#include <gtest/gtest.h>
#include <atomic>

#include "utils/logging/logging.h"
#include "utils/random/random.h"

namespace runai::llm::streamer::utils
{

TEST(Creation, Sanity)
{
    for (auto size : { 1U, 0U, utils::random::number(2, 100), 100U })
    {
        ThreadPool<unsigned> pool([](unsigned){}, size);
        (void)pool;
    }
}

TEST(Handle, Sanity)
{
    for (auto size : { 1U, 2U, 10U, utils::random::number(10, 100), 100U })
    {
        constexpr auto Options = 1000;

        std::array<std::atomic<unsigned>, Options> counters{};
        std::atomic<unsigned> total = 0;

        ThreadPool<unsigned> pool([&](unsigned index)
            {
                total++;
                counters[index]--;
            }, size);

        const auto count = utils::random::number(50000, 100000);

        for (unsigned i = 0; i < count; ++i)
        {
            auto index = utils::random::number(Options);
            counters[index]++;
            pool.push(std::move(index));
        }

        sleep(1);

        EXPECT_EQ(total, count);

        for (const auto & counter : counters)
        {
            EXPECT_EQ(counter, 0);
        }
    }
}

TEST(Handle, Exception_Thrown_From_Handler)
{
    for (auto size : { 1U, 2U, 10U, utils::random::number(10, 100), 100U })
    {
        std::atomic<unsigned> counter = 0;

        ThreadPool<bool> pool([&](auto _)
            {
                counter++;
                throw std::exception();
            }, size);

        const auto count = utils::random::number(50, 100);

        for (unsigned i = 0; i < count; ++i)
        {
            pool.push(utils::random::boolean());
        }

        sleep(1);

        EXPECT_EQ(count, counter);
    }
}

} // namespace runai::llm::streamer::utils
