#include "common/responder/responder.h"

#include <gtest/gtest.h>
#include <set>
#include <utility>

#include "utils/random/random.h"
#include "utils/threadpool/threadpool.h"
#include "utils/semaphore/semaphore.h"

namespace runai::llm::streamer::common
{

TEST(Creation, Empty)
{
    auto responder = Responder(0);

    auto times = utils::random::number(1, 10);
    for (unsigned i = 0; i < times; ++i)
    {
        auto r = responder.pop();
        EXPECT_EQ(r.ret, ResponseCode::FinishedError);
    }
}

TEST(Destruction, Sanity)
{
    auto size = utils::random::number(1, 100);
    auto responder = Responder(size);

    for (unsigned i = 0; i < size; ++i)
    {
        responder.push(i);
    }

    // D'tor waits until all tasks are finished
}

TEST(Pop, Sanity)
{
    auto size = utils::random::number(1, 100);
    auto responder = Responder(size);

    for (unsigned i = 0; i < size; ++i)
    {
        responder.push(i);
    }

    for (unsigned i = 0; i < size; ++i)
    {
        auto r = responder.pop();
        EXPECT_EQ(r.index, i);
        EXPECT_EQ(r.ret, ResponseCode::Success);
    }

    auto times = utils::random::number(1, 10);
    for (unsigned i = 0; i < times; ++i)
    {
        auto r = responder.pop();
        EXPECT_EQ(r.ret, ResponseCode::FinishedError);
    }
}

TEST(Pop, Wait)
{
    auto size = utils::random::number(1, 100);
    auto responder = Responder(size);

    // create threadpool to push
    auto pool = utils::ThreadPool<unsigned>([&](unsigned i)
    {
        responder.push(i);
    }, size);

    std::set<unsigned> expected;
    for (unsigned i = 0; i < size; ++i)
    {
        expected.insert(i);
        unsigned value = i;
        pool.push(std::move(value));
    }

    // wait for all the responses

    for (unsigned i = 0; i < size; ++i)
    {
        auto r = responder.pop();
        EXPECT_EQ(r.ret, ResponseCode::Success);
        EXPECT_EQ(expected.count(r.index), 1);
        expected.erase(r.index);
    }

    EXPECT_EQ(expected.size(), 0);

    auto times = utils::random::number(1, 10);
    for (unsigned i = 0; i < times; ++i)
    {
        auto r = responder.pop();
        EXPECT_EQ(r.ret, ResponseCode::FinishedError);
    }
}

TEST(Pop, Error)
{
    for (auto rc : {ResponseCode::FileAccessError, ResponseCode::EofError, ResponseCode::UnknownError} )
    {
        auto size = utils::random::number(1, 3);

        // create a responder for the expected responses

        auto responder = Responder(size);

        // create threadpool to push
        auto pool = utils::ThreadPool<int>([&](int i)
        {
            auto r = Response(rc);
            responder.push(std::move(r));
        }, size);

        for (unsigned i = 0; i < size; ++i)
        {
            unsigned value = i;
            pool.push(std::move(value));
        }

        // wait for all the responses

        for (unsigned i = 0; i < size; ++i)
        {
            auto r = responder.pop();
            EXPECT_EQ(r.ret, rc);
        }

        auto times = utils::random::number(1, 10);
        for (unsigned i = 0; i < times; ++i)
        {
            auto r = responder.pop();
            EXPECT_EQ(r.ret, ResponseCode::FinishedError);
        }
    }
}

TEST(Pop, Unexpected_Responses)
{
    auto size = utils::random::number(2, 100);
    const auto expected = utils::random::number(1, size-1);
    auto responder = Responder(expected);

    // create threadpool to push

    std::atomic<unsigned> completed = 0;
    auto finished = utils::Semaphore(0);
    auto pool = utils::ThreadPool<unsigned>([&](unsigned i)
    {
        responder.push(i);
        completed++;
        if (completed == size)
        {
            finished.post();
        }
    }, size);

    for (unsigned i = 0; i < size; ++i)
    {
        auto value = i;
        pool.push(std::move(value));
    }

    // wait until all the responses are pushed
    finished.wait();

    unsigned error_responses = 0;
    unsigned success_responses = 0;

    // wait for all the responses

    for (unsigned i = 0; i < size; ++i)
    {
        auto r = responder.pop();
        if (r.ret == ResponseCode::Success)
        {
            ++success_responses;
        }
        else
        {
            EXPECT_EQ(r.ret, ResponseCode::UnknownError);
            ++error_responses;
        }
    }

    EXPECT_EQ(success_responses, expected);
    EXPECT_EQ(error_responses, size - expected);

    auto times = utils::random::number(1, 10);
    for (unsigned i = 0; i < times; ++i)
    {
        auto r = responder.pop();
        EXPECT_EQ(r.ret, ResponseCode::FinishedError);
    }
}

}; // namespace runai::llm::streamer::common
