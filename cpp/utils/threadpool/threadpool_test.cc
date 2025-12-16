#include "utils/threadpool/threadpool.h"

#include <unistd.h>

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
        ThreadPool<unsigned> pool([](unsigned, std::atomic<bool> &){}, size);
        (void)pool;
    }
}

TEST(Handle, Sanity)
{
    for (auto size : { 1U, 2U, 10U, utils::random::number(10, 100), 100U })
    {
        constexpr auto Options = 1000;

        std::array<std::atomic<int>, Options> counters{};
        std::atomic<unsigned> total = 0;

        ThreadPool<unsigned> pool([&](unsigned index, std::atomic<bool> & stopped)
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

        ThreadPool<bool> pool([&](auto _, std::atomic<bool> & stopped)
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

TEST(Handle, Stopped)
{
    for (auto size : { 1U, 2U, 10U, utils::random::number(10, 100), 100U })
    {
        std::atomic<unsigned> total = 0;
        const auto count = utils::random::number(5000, 10000);

        {
            ThreadPool<unsigned> pool([&](auto _, std::atomic<bool> & stopped)
                {
                    while (!stopped)
                    {
                        ::usleep(utils::random::number(10000));
                    }
                    total++;
                }, size);

            for (unsigned i = 0; i < count; ++i)
            {
                pool.push(utils::random::boolean());
            }

            ::usleep(utils::random::number(100000, 1000000));

            EXPECT_EQ(total, 0);
        }

        // verify all threads stopped and each thread performed a single task

        EXPECT_EQ(total, size);
    }
}

} // namespace runai::llm::streamer::utils
