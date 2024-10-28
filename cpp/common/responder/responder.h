
#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>

#include "utils/semaphore/semaphore.h"
#include "common/response/response.h"

namespace runai::llm::streamer::common
{

// Responder to hold and return Response objects
// Initialized with the expected number of responses

// Maintains a queue of responses, each response corresponds to a Request object which represents a sub range (e.g. tensor data)
// Implements producer-consumer design:
//    push: ready responses are pushed to the queue and notify the semaphore
//    pop : waits on the semaphore and returns the first response in the queue
//          if no responses are expected returns ResponseCode::FinishedError

// Designed for multi producers that push responses and a single consumer that is waiting for responses

struct Responder
{
    Responder(unsigned running);

    ~Responder();

    Response pop();

    void push(Response && response);
    void push(Response && response, size_t bytesize);

    void cancel();

    bool finished() const;

    // return throughput in bytes per second
    size_t bytes_per_second() const;

 private:
    // expected number of responses
    unsigned _running;

    // responses for completed requests
    std::deque< Response > _responses;

    // signals for responses in the queue
    utils::Semaphore _ready;

    // mutex to make the queue thread safe
    mutable std::mutex _mutex;

    bool _canceled = false;

    std::atomic<size_t> _total_bytesize;
    std::chrono::time_point<std::chrono::steady_clock> _start_time;

    bool _successful = true;
};

}; // namespace runai::llm::streamer::common
