#include "utils/semaphore/semaphore.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::utils
{

Semaphore::Semaphore(unsigned value)
{
    PASSERT(sem_init(&_sem, 0, value) == 0) << "Failed creating semaphore";
}

Semaphore::~Semaphore()
{
    CHECK(sem_destroy(&_sem) == 0) << "Failed destroying semaphore";
}

void Semaphore::post()
{
    PASSERT(sem_post(&_sem) == 0) << "Could not increment semaphore";
}

void Semaphore::wait()
{
    // restart if interrupted by signal
    int ret{};
    while ((ret = sem_wait(&_sem)) == -1 && errno == EINTR)
    {
        LOG(SPAM) << "Received EINTR while waiting on semaphore " << reinterpret_cast<void*>(&_sem);
        continue;
    }

    PASSERT(ret == 0) << "Could not decrement semaphore";
}

unsigned Semaphore::value()
{
    int value;
    PASSERT(sem_getvalue(&_sem, &value) == 0) << "Could not get semaphore value";

    // in Linux, `value` is (0) and not a negative value
    // for more information read `man sem_getvalue`
    return static_cast<unsigned>(value);
}

} // namespace runai::llm::streamer::utils
