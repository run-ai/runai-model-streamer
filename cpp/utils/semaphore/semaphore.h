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

    // get the semaphore value
    unsigned value();

 private:
    sem_t _sem;
};

} // namespace runai::llm::streamer::utils
