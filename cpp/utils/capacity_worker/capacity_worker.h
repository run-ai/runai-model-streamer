#pragma once

#include <atomic>
#include <cstddef>
#include <utility>

#include "utils/threadpool/threadpool.h"          // Worker<Request>
#include "utils/capacity_queue/capacity_queue.h"

namespace runai::llm::streamer::utils
{

// CapacityWorker - a Worker (see threadpool.h) for asynchronous backends. It owns a CapacityQueue (the
// in-flight window) and implements execute/drain/idle as the submit+drain interleave pattern, so a
// concrete worker only fills three backend I/O hooks. `Chunk` is the unit submitted to the backend and
// held in the window.
//
//  * execute submits a request into the window, draining internally to make room until the request is
//    fully submitted, then drains once more to free a slot - so it returns ready for the next request and
//    can never stall on a full window.
//  * drain progresses the tail one batch at a time (so a newly arrived request can interleave - the pool's
//    worker_routine re-checks the queue between drains).
//  * idle is true once nothing is pending or in flight.
//
// The pattern lives here once; concrete workers (e.g. object storage) implement only enqueue/submit/
// drain_batch. Backends that don't need a window (filesystem) don't derive from this at all - they use
// ThreadPool's stateless Handler constructor.
template <typename Request, typename Chunk>
class CapacityWorker : public Worker<Request>
{
 public:
    explicit CapacityWorker(std::size_t capacity) :
        _queue(capacity)
    {}

    void execute(Request && request, std::atomic<bool> & stopped) final override
    {
        enqueue(std::move(request));                    // chunk the request into _queue, track it in flight
        while (!_queue.empty())                         // pending = this request's not-yet-submitted chunks
        {
            pump();                                     // submit up to capacity
            if (!_queue.empty())
            {
                drain_batch(stopped);                   // window full -> free slots, then submit more
            }
        }
        if (_queue.inflight() >= _queue.capacity())
        {
            drain_batch(stopped);                       // fully submitted but full -> free a slot for the next
        }
    }

    void drain(std::atomic<bool> & stopped) final override
    {
        drain_batch(stopped);                           // one batch, then the pool re-checks for new work
        pump();                                         // submit whatever the freed slots now allow
    }

    bool idle() const final override
    {
        return _queue.idle();
    }

 protected:
    // Split `request` into chunks, _queue.enqueue(...) each, and register it as in flight with whatever
    // per-request/per-task tracking drain_batch needs to route completions to it and finalize it.
    virtual void enqueue(Request && request) = 0;

    // Submit one chunk to the backend (fire the async read).
    virtual void submit(const Chunk & chunk) = 0;

    // Process the completions ready now: for each, _queue.complete(cost), route it to its owning request,
    // and finalize+drop the request once its last chunk lands. Blocks for at least one completion when the
    // window is full (nothing else to do); otherwise takes whatever is ready.
    virtual void drain_batch(std::atomic<bool> & stopped) = 0;

    CapacityQueue<Chunk> _queue;

 private:
    void pump()
    {
        while (auto chunk = _queue.try_take())
        {
            submit(*chunk);
        }
    }
};

}; // namespace runai::llm::streamer::utils
