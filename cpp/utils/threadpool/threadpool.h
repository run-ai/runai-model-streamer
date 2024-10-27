#pragma once

#include <utility>
#include <vector>
#include <string>
#include <deque>
#include <mutex>

#include "utils/logging/logging.h"
#include "utils/thread/thread.h"
#include "utils/semaphore/semaphore.h"

namespace runai::llm::streamer::utils
{

template <typename Message>
struct Deque
{
    void push(Message && message)
    {
        {
            const auto lock = std::unique_lock<std::mutex>(_mutex);

            ASSERT(!_stopped) << "Pushing a message to an already stopped queue";

            _deque.push_back(std::move(message));
        }

        _sem.post(); // notify about the new message
    }

    bool pop(/* out */ Message & message)
    {
        _sem.wait(); // wait for a message

        const auto lock = std::unique_lock<std::mutex>(_mutex);

        if (_stopped)
        {
            return false;
        }

        /* out */ message = std::move(_deque.front());
        _deque.pop_front();

        return true;
    }

    // any unresolved messages in the deque will be dropped
    void stop(unsigned times) // `times` is the number of times to increment the semaphore
    {
        {
            const auto lock = std::unique_lock<std::mutex>(_mutex);

            if (_deque.size() != 0)
            {
                LOG(DEBUG) << "Stopping a `Deque` with unresolved messages";
            }
            _stopped = true;
        }

        for (unsigned i = 0; i < times; ++i)
        {
            _sem.post();
        }
    }

    unsigned size() const // get the current size of the deque
    {
        const auto lock = std::unique_lock<std::mutex>(_mutex);

        return _deque.size();
    }

 private:
    Semaphore _sem = 0; // no messages are available
    std::deque<Message> _deque;
    bool _stopped = false;
    mutable std::mutex _mutex; // guarding `_deque` and `_stopped`
};

template <typename Request>
struct ThreadPool
{
    using Handler = std::function<void(Request &&, std::atomic<bool> &)>;

    ThreadPool(const Handler & handler, unsigned size) :
        stopped(false),
        _handler(handler)
    {
        _threads.reserve(size);
        for (unsigned i = 0; i < size; ++i)
        {
            _threads.emplace_back(std::bind(routine, std::ref(*this)));
        }
    }

    ~ThreadPool()
    {
         // stop the deque and notify all worker threads
        _deque.stop(_threads.size());
        stopped = true;
    }

    void push(Request && request)
    {
        _deque.push(std::move(request));
    }

    static void routine(ThreadPool & pool)
    {
        while (true)
        {
            Request request;
            if (!pool._deque.pop(/* out */ request))
            {
                break;
            }

            try
            {
                pool._handler(std::move(request), pool.stopped);
            }
            catch (...)
            {
                LOG(WARNING) << "Failed handling request";
            }
        }
    }

    std::atomic<bool> stopped;

 private:
    Handler _handler;
    Deque<Request> _deque;
    std::vector<Thread> _threads;
};

} // namespace runai::llm::streamer::utils
