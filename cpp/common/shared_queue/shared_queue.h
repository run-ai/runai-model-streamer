#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <type_traits> // For std::is_constructible
#include <typeinfo>
#include <utility>

#include "utils/semaphore/semaphore.h"
#include "common/response_code/response_code.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::common
{

// SharedQueue to hold and return ResponseType objects
// Initialized with the expected number of responses

// ResponseType object must be initialized with a common::ResponseCode
// ResponseType object must have a member 'ret' of type common::ResponseCode
// ResponseType must be movable
// ResponseType must be streamable to std::ostream

// Implements producer-consumer design:
//    push: ready responses are pushed to the queue and notify the semaphore
//    pop : waits on the semaphore and returns the first response in the queue
//          if no responses are expected returns a ResponseType indicating FinishedError

// Designed for multi producers that push responses and a single consumer that is waiting for responses

template <typename ResponseType>
struct SharedQueue
{
    // Static assertion to ensure ResponseType has the expected 'ret' member and constructor.
    // This is a basic check; more sophisticated checks might use SFINAE or concepts in C++20.
    static_assert(std::is_constructible<ResponseType, common::ResponseCode>::value,
                  "ResponseType must be constructible from common::ResponseCode for error states.");

    SharedQueue(unsigned running);

    ~SharedQueue();

    void increment(unsigned running);

    ResponseType pop();

    void push(ResponseType && response);
    void push(ResponseType && response, size_t bytesize);

    void cancel();
    void stop();

    bool finished() const;

    // return throughput in bytes per second
    size_t bytes_per_second() const;

    common::ResponseCode valid() const;

 private:
    // expected number of responses
    unsigned _running;

    // responses for completed requests
    std::deque< ResponseType > _responses;

    // signals for responses in the queue
    utils::Semaphore _ready;

    // mutex to make the queue thread safe
    mutable std::mutex _mutex;

    bool _canceled = false;
    std::atomic<bool> _stopped;

    std::atomic<size_t> _total_bytesize;
    std::chrono::time_point<std::chrono::steady_clock> _start_time;

    bool _successful = true;

    std::atomic<bool> _unexpected_push_error;
};


// --- Implementation ---

template <typename ResponseType>
SharedQueue<ResponseType>::SharedQueue(unsigned running) :
    _running(running),
    _ready(0), // Semaphore initialized to 0
    _stopped(false),
    _total_bytesize(0),
    _start_time(std::chrono::steady_clock::now()),
    _unexpected_push_error(false)
{
    LOG(DEBUG) << "created responder for " << running << " running requests (Type: " << typeid(ResponseType).name() << ")";
}

template <typename ResponseType>
SharedQueue<ResponseType>::~SharedQueue()
{
    LOG(DEBUG) << "Responder shutdown";
}

template <typename ResponseType>
void SharedQueue<ResponseType>::increment(unsigned running)
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    _running += running;
    LOG(DEBUG) << "Responder incremented, new running count: " << _running;
}

template <typename ResponseType>
ResponseType SharedQueue<ResponseType>::pop()
{
    if (_stopped.load(std::memory_order_acquire) || finished()) // Use acquire for atomic read
    {
        LOG(DEBUG) << (_stopped.load(std::memory_order_relaxed) ? "responder stopped" : "responder does not expect any more responses") << " (Type: " << typeid(ResponseType).name() << ")";
        return ResponseType(common::ResponseCode::FinishedError);
    }

    _ready.wait(); // Wait for a response to be pushed

    const auto guard = std::unique_lock<std::mutex>(_mutex);

    if (_stopped.load(std::memory_order_relaxed))
    {
        LOG(DEBUG) << "responder stopped while waiting or after acquiring lock (Type: " << typeid(ResponseType).name() << ")";
        return ResponseType(common::ResponseCode::FinishedError);
    }

    ASSERT(!_responses.empty()) << "responder is empty after notification. Current running " << _running << " (Type: " << typeid(ResponseType).name() << ")";

    ResponseType response = std::move(_responses.front());
    LOG(SPAM) << "Sending response: " << response; // Requires ResponseType to be streamable
    _responses.pop_front();

    return response;
}

template <typename ResponseType>
void SharedQueue<ResponseType>::push(ResponseType && response)
{
    bool posted_to_semaphore = false;
    {
        const auto guard = std::unique_lock<std::mutex>(_mutex);

        if (_stopped.load(std::memory_order_relaxed))
        {
            LOG(DEBUG) << "Responder stopped, ignoring pushed response (Type: " << typeid(ResponseType).name() << ")";
            // ignore responses
            return;
        }

        // Assuming ResponseType has a member 'ret' of type common::ResponseCode
        _successful  = _successful && (response.ret == common::ResponseCode::Success);

        if (_running > 0) // Check _running before decrementing
        {
            LOG(SPAM) << response << " ; " << _running << " running requests (Type: " << typeid(ResponseType).name() << ")";
            _responses.push_back(std::move(response)); // Use std::move
            --_running;
            posted_to_semaphore = true;

            if (_running == 0 && _successful && _total_bytesize.load(std::memory_order_relaxed) > 100 * 1024 * 1024)
            {
                const auto throughput = bytes_per_second(); // bytes_per_second uses _mutex internally if needed
                // Consider if std::cout is appropriate here or if it should be logged.
                // This might also be better placed outside the lock if bytes_per_second is slow.
                std::cout << "Read throughput is " << utils::logging::human_readable_size(throughput) << " per second " << std::endl;
            }
        }
        else
        {
            LOG(ERROR) << "Received unexpected response (no running requests) " << response << " (Type: " << typeid(ResponseType).name() << ")";
            _unexpected_push_error.store(true, std::memory_order_relaxed);
        }
    } // Mutex guard released

    if (posted_to_semaphore)
    {
        _ready.post(); // Signal that a response is ready
    }
}

template <typename ResponseType>
void SharedQueue<ResponseType>::push(ResponseType && response, size_t bytesize)
{
    _total_bytesize.fetch_add(bytesize, std::memory_order_relaxed);
    push(std::move(response)); // Forward to the other push method
}

template <typename ResponseType>
bool SharedQueue<ResponseType>::finished() const
{
    const auto guard = std::unique_lock<std::mutex>(_mutex);
    // A responder is finished if it was canceled,
    // OR if all expected responses have been accounted for (_running == 0)
    // AND there are no more responses waiting in the queue to be popped.
    return (_canceled || (_running == 0 && _responses.empty()));
}

template <typename ResponseType>
void SharedQueue<ResponseType>::cancel()
{
    bool needs_post = false;
    {
        const auto guard = std::unique_lock<std::mutex>(_mutex);
        if (!_canceled && !_stopped.load(std::memory_order_relaxed)) {
             _canceled = true;
             // If there are threads potentially blocked in pop() waiting on _ready,
             // and finished() will now return true, we might need to signal them
             // to wake up and re-check the finished() condition.
             if (_running > 0 || !_responses.empty()) { // If pop() could be waiting
                 needs_post = true;
             }
        }
        LOG(DEBUG) << "Responder canceled. Current running: " << _running << ", Responses in queue: " << _responses.size() << " (Type: " << typeid(ResponseType).name() << ")";
    }
    if (needs_post)
    {
        _ready.post();
    }
}

template <typename ResponseType>
void SharedQueue<ResponseType>::stop()
{
    bool needs_post = false;
    {
        LOG(DEBUG) << "Stopping responder..." << " (Type: " << typeid(ResponseType).name() << ")";
        const auto guard = std::unique_lock<std::mutex>(_mutex);
        if (!_stopped.exchange(true, std::memory_order_acq_rel)) { // Ensure stop is only processed once
            // If there are threads potentially blocked in pop() waiting on _ready,
            // we need to signal them to wake up and see the _stopped flag.
            needs_post = true; // Post regardless of _running, to unblock any waiter
        }
    }

    if (needs_post)
    {
        // For our "single consumer" design, one post is okay.
        _ready.post();
        LOG(DEBUG) << "Responder stopped and semaphore posted (Type: " << typeid(ResponseType).name() << ")";
    }
    else
    {
        LOG(DEBUG) << "Responder already stopped or stop initiated by another thread (Type: " << typeid(ResponseType).name() << ")";
    }
}

template <typename ResponseType>
size_t SharedQueue<ResponseType>::bytes_per_second() const
{
    // This function might be called from push() which holds the mutex.
    //  _start_time is only set in constructor
    // _total_bytesize is atomic
    const auto time_ = std::chrono::steady_clock::now();
    const auto duration  = std::chrono::duration_cast<std::chrono::milliseconds>(time_ - _start_time);
    const auto milliseconds = duration.count();

    if (milliseconds == 0)
    {
        return 0; // Avoid division by zero
    }

    LOG(DEBUG) << "Read " << utils::logging::human_readable_size(_total_bytesize.load(std::memory_order_relaxed))
               << " in " << milliseconds << " milliseconds (Type: " << typeid(ResponseType).name() << ")";
    return static_cast<double>(_total_bytesize.load(std::memory_order_relaxed)) / (static_cast<double>(milliseconds) / 1000.0);
}

template <typename ResponseType>
common::ResponseCode SharedQueue<ResponseType>::valid() const
{
    return _unexpected_push_error.load(std::memory_order_relaxed) ? common::ResponseCode::UnknownError : common::ResponseCode::Success;
}

} // namespace runai::llm::streamer::common
