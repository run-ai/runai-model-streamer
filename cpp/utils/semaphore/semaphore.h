#pragma once

#include <semaphore.h>

namespace runai::llm::streamer::utils
{

struct Semaphore
{
    Semaphore(unsigned value);
    ~Semaphore();

    // increment and decrement the semaphore
    void post();
    void wait();

    // wait up to timeout_ms for the semaphore; returns true if acquired (decremented),
    // false on timeout. Uses an absolute CLOCK_REALTIME deadline (sem_timedwait); a
    // wall-clock jump can skew the wait (CLOCK_MONOTONIC via sem_clockwait needs glibc 2.30).
    bool wait_for(unsigned timeout_ms);

    // get the semaphore value
    unsigned value();

 private:
    sem_t _sem;
};

} // namespace runai::llm::streamer::utils
