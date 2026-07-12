#include "utils/semaphore/semaphore.h"

#include <gtest/gtest.h>

#include <chrono>

#include "utils/random/random.h"
#include "utils/thread/thread.h"

namespace runai::llm::streamer::utils
{

TEST(Creation, Sanity)
{
    const auto number = random::number();

    Semaphore sem(number);

    EXPECT_EQ(sem.value(), number);
}

TEST(Wait, Sanity)
{
    const auto number = random::number(1, 10);

    auto sem = Semaphore(number);

    EXPECT_EQ(sem.value(), number);

    for (unsigned i = 0; i < number; ++i)
    {
        sem.wait();
    }

    EXPECT_EQ(sem.value(), 0);
}

TEST(Wait, Actually_Wait)
{
    auto sem = Semaphore(0);

    const auto start = std::chrono::steady_clock::now();

    auto thread = Thread([&sem]()
    {
        ::sleep(1);

        sem.post();
    });

    sem.wait();

    const auto end = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    EXPECT_GT(duration.count(), 900);
}

TEST(WaitFor, Timeout)
{
    auto sem = Semaphore(0);

    const auto start = std::chrono::steady_clock::now();
    const bool acquired = sem.wait_for(100);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

    EXPECT_FALSE(acquired);
    EXPECT_GE(ms, 90); // actually waited ~the timeout, did not return early
}

TEST(WaitFor, AlreadyAvailable)
{
    const auto number = random::number(1, 10);
    auto sem = Semaphore(number);

    // a positive semaphore is acquired immediately, without waiting
    const auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(sem.wait_for(10000));
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

    EXPECT_LT(ms, 1000);
    EXPECT_EQ(sem.value(), number - 1);
}

TEST(WaitFor, PostedBeforeTimeout)
{
    auto sem = Semaphore(0);

    auto thread = Thread([&sem]()
    {
        usleep(100 * 1000); // 100ms, well within the timeout
        sem.post();
    });

    EXPECT_TRUE(sem.wait_for(5000));
}

} // namespace runai::llm::streamer::utils
