#include "common/posix_io/libaio_engine/libaio_engine.h"

#include <time.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "common/exception/exception.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::posix_io
{

namespace
{

// libaio returns -errno rather than setting errno, the same way liburing does. Named because `-ret`
// reads as arithmetic everywhere it appears otherwise.
int error_of(int ret)
{
    return -ret;
}

uint64_t now_nanos()
{
    struct timespec ts;

    // CLOCK_MONOTONIC goes through the vDSO, so this is tens of nanoseconds and no syscall. It runs
    // once per flush(), and a flush carries a whole batch, so it does not show at our rates.
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
}

// Set up an aio context of at most `wanted` events, and report what was granted.
//
// io_setup refuses with EAGAIN when aio-nr + maxevents would pass /proc/sys/fs/aio-max-nr. That
// number is node wide and other pods draw on it, so the size we ask for is not ours to assume.
//
// Halving rather than reading the proc files: their values are shared, so anything read there can be
// stale by the time we call io_setup. Asking is the only answer that cannot be raced.
unsigned setup_context(unsigned wanted, io_context_t & out_ctx)
{
    if (wanted == 0)
    {
        // Caught here so the message names the real problem. io_setup(0) returns EINVAL - measured -
        // and the loop below would skip entirely, so without this the failure would be reported as
        // "the node could not give us one event", which is not what happened.
        LOG(ERROR) << "libaio was asked for a depth of 0";
        throw common::Exception(ResponseCode::UnknownError);
    }

    for (unsigned events = wanted; events >= 1; events /= 2)
    {
        // io_setup needs a zeroed context. A non-zero value makes it return EINVAL - measured - and a
        // failed attempt may have written to the variable, so it is reset on every pass.
        io_context_t ctx = nullptr;

        const int ret = io_setup(static_cast<int>(events), &ctx);
        if (ret == 0)
        {
            out_ctx = ctx;
            return events;
        }

        if (error_of(ret) != EAGAIN)
        {
            LOG(ERROR) << "io_setup(" << events << ") failed: " << std::strerror(error_of(ret));
            throw common::Exception(ResponseCode::UnknownError);
        }

        LOG(WARNING) << "io_setup(" << events << ") was refused because the node's aio limit is"
                     << " reached (/proc/sys/fs/aio-max-nr, shared with everything else on this"
                     << " node); trying " << events / 2;
    }

    LOG(ERROR) << "io_setup could not provide even one event";
    throw common::Exception(ResponseCode::UnknownError);
}

} // namespace

LibaioEngine::LibaioEngine(const AsyncIoConfig & config, size_t max_read_bytesize)
{
    static_assert(sizeof(void *) >= sizeof(RequestId),
                  "the request id travels in iocb->data, which is a void *");

    _depth = setup_context(config.depth, _ctx);

    if (_depth != config.depth)
    {
        // Reported here, at construction, and not left to be met later as backpressure. A context
        // smaller than the configured depth means the depth silently never happens, and the only
        // symptom is a slow read - which reads as a slow disk rather than as a limit being hit
        // (design 5.9.1).
        LOG(WARNING) << "libaio depth clamped from " << config.depth << " to " << _depth
                     << ". Raise /proc/sys/fs/aio-max-nr on the node, or lower the configured depth"
                     << " so it is not discovered as backpressure";
    }

    // Sized once and never resized. The kernel keeps the address of each iocb until its read
    // completes, so a resize would move the structs and leave the kernel holding freed memory.
    _iocbs.resize(_depth);

    _free.reserve(_depth);
    for (auto & iocb : _iocbs)
    {
        _free.push_back(&iocb);
    }

    _pending.reserve(_depth);
    _events.resize(_depth);

    _limits.max_read_bytesize = max_read_bytesize;

    // What a direct read on this host requires. The same numbers and the same reasoning as
    // IoUringEngine: statx reports the real values only from kernel 6.1, our floor is 5.15, and 4096
    // is a safe superset of every block size in use. Over-aligning can waste, never fail.
    //
    // Reporting 1 would be far worse than wasteful. The caller tests congruence against this number,
    // and everything is congruent modulo 1, so every file would be opened with O_DIRECT and every
    // unaligned read would then fail with EINVAL.
    _limits.offset_alignment = 4096;
    _limits.buffer_alignment = 4096;

    LOG(INFO) << "libaio ready: " << _depth << " events";
}

LibaioEngine::LibaioEngine(const AsyncIoConfig & config) :
    LibaioEngine(config, max_read_bytesize())
{}

LibaioEngine::~LibaioEngine()
{
    // How much time submitting cost, logged once. This is the number that says whether keeping one
    // thread was right, or whether a submit thread has to be designed - see SubmitStats.
    if (_submit_stats.calls != 0)
    {
        LOG(INFO) << "libaio io_submit: " << _submit_stats.calls << " calls carrying "
                  << _submit_stats.iocbs << " reads, " << _submit_stats.nanos / 1000
                  << " us in total, worst call " << _submit_stats.max_nanos / 1000 << " us";
    }

    // io_destroy MUST NOT run with reads in flight: the kernel would be left writing into
    // destinations the caller believes are free. The caller quiesces first (io_engine.h), so
    // reaching here with anything outstanding is its failure, not something to repair.
    if (_ctx != nullptr)
    {
        const int ret = io_destroy(_ctx);
        if (ret < 0)
        {
            LOG(ERROR) << "io_destroy failed: " << std::strerror(error_of(ret));
        }
    }
}

Limits LibaioEngine::limits() const
{
    return _limits;
}

unsigned LibaioEngine::depth() const
{
    return _depth;
}

const SubmitStats & LibaioEngine::submit_stats() const
{
    return _submit_stats;
}

ResponseCode LibaioEngine::stage(RequestId id, FileRef file, size_t offset, size_t bytesize, char * buffer)
{
    if (_free.empty())
    {
        // Unreachable by construction: the caller's window is sized from depth(), and every staged
        // read holds a credit until its completion, so outstanding plus staged can never pass the
        // number of iocbs. Reaching here means that invariant broke somewhere else.
        //
        // Reported rather than asserted, and not repaired by an internal flush - the same reasoning
        // as IoUringEngine::stage. ASSERT is fatal in every build here, and an internal submit would
        // put reads in flight that the caller's accounting cannot see.
        LOG(ERROR) << "libaio has no free iocb at depth " << _depth
                   << " - the in-flight window and the context have disagreed";
        return ResponseCode::UnknownError;
    }

    // file.direct is not read here, and that is not an oversight. io_uring needs it to decide
    // IOSQE_ASYNC; libaio has no equivalent knob, and O_DIRECT is already a property of the fd the
    // caller opened. A buffered fd is accepted and read correctly - it is just read synchronously
    // inside io_submit, which is why the caller routes buffered files away from this engine rather
    // than letting them arrive here.

    struct iocb * iocb = _free.back();
    _free.pop_back();

    io_prep_pread(iocb, file.fd, buffer, bytesize, static_cast<long long>(offset));
    iocb->data = reinterpret_cast<void *>(static_cast<uintptr_t>(id));

    _pending.push_back(iocb);
    return ResponseCode::Success;
}

ResponseCode LibaioEngine::flush(unsigned & out_issued)
{
    out_issued = 0;

    if (_pending.empty())
    {
        return ResponseCode::Success;
    }

    const uint64_t started = now_nanos();
    const int ret = io_submit(_ctx, static_cast<long>(_pending.size()), _pending.data());
    const uint64_t elapsed = now_nanos() - started;

    ++_submit_stats.calls;
    _submit_stats.nanos += elapsed;
    _submit_stats.max_nanos = std::max(_submit_stats.max_nanos, elapsed);

    if (ret < 0)
    {
        const int error = error_of(ret);

        // This is backpressure. EAGAIN from io_submit means the aio context is full, which means reads
        // are in flight, which means completions are coming and reaping will free room. The staged
        // reads keep their place at the head and go out on the next flush, in order.
        //
        // Reported here rather than retried in a loop. Only reaping frees capacity, and reaping runs
        // on this same thread, so a loop would wait for an event that only it could cause (design
        // 5.9).
        //
        // EINTR is handled the same way, defensively. It is not documented for io_submit and has not
        // been seen, but any negative return means nothing was accepted, so retrying the untouched
        // array is right whatever the errno.
        if (error == EAGAIN || error == EINTR)
        {
            LOG(DEBUG) << "io_submit deferred " << _pending.size() << " staged reads: "
                       << std::strerror(error);
            return ResponseCode::Success;
        }

        // io_submit stops at the first read it cannot accept and reports how many it took. So a
        // negative return is about the head of the queue and only the head: everything behind it was
        // never looked at.
        //
        // That one read is taken out and answered as a completion carrying -errno, which is the form
        // io_uring uses for the same problem. Leaving it in place would block everything behind it for
        // good, and failing the whole flush makes the worker abort every read on this engine
        // (async_io_worker.cc, abort_all).
        //
        // Success with out_issued == 0: nothing went out, and the rest of the queue is retried on the
        // next flush. This always finishes, because each pass removes one read.
        struct iocb * head = _pending.front();

        Completion completion;
        completion.id = static_cast<RequestId>(reinterpret_cast<uintptr_t>(head->data));
        completion.res = -static_cast<long>(error);

        LOG(ERROR) << "io_submit refused read " << completion.id << ": " << std::strerror(error)
                   << ". Reporting it as a failed read and keeping the other " << _pending.size() - 1
                   << " staged";

        _submit_failures.push_back(completion);
        _pending.erase(_pending.begin());
        _free.push_back(head);

        return ResponseCode::Success;
    }

    const size_t issued = static_cast<size_t>(ret);

    ASSERT(issued <= _pending.size()) << "io_submit accepted " << issued << " of "
                                      << _pending.size() << " staged";

    _submit_stats.iocbs += issued;
    out_issued = static_cast<unsigned>(issued);

    // Drop the accepted prefix so the unissued head is _pending[0] again. Erasing from the front of
    // a vector moves the rest, which is at most `depth` pointers and only on the partial path; the
    // common case is the clear() below.
    if (issued == _pending.size())
    {
        _pending.clear();
    }
    else
    {
        _pending.erase(_pending.begin(), _pending.begin() + issued);
    }

    return ResponseCode::Success;
}

ResponseCode LibaioEngine::wait_for_completions(Completion * out, unsigned max, unsigned & out_count,
                                                WaitMode mode, unsigned timeout_ms)
{
    out_count = 0;

    if (max == 0)
    {
        return ResponseCode::Success;
    }

    // Reads the kernel refused, handed back before anything else. If they waited until after the
    // kernel wait, a blocking wait with nothing in flight would sit here for the whole timeout while
    // a completion was already available in this vector.
    if (!_submit_failures.empty())
    {
        const size_t taken = std::min<size_t>(max, _submit_failures.size());
        std::copy(_submit_failures.begin(), _submit_failures.begin() + taken, out);
        _submit_failures.erase(_submit_failures.begin(), _submit_failures.begin() + taken);
        out_count = static_cast<unsigned>(taken);
        return ResponseCode::Success;
    }

    // min_nr and nr are different questions. min_nr is how many completions to WAIT for; nr is how
    // many to return. So min_nr == 1 means "do not wait for a second one", and the call still returns
    // every completion that is ready - measured, with five reads finished it returns all five.
    //
    // nr is therefore the full depth, so one wait collects the whole backlog. Sizing it to 1 would
    // cost one syscall per read instead of one per batch, and the only symptom would be less
    // throughput.
    const long nr = static_cast<long>(std::min<size_t>(max, _events.size()));

    const long min_nr = (mode == WaitMode::Block) ? 1 : 0;

    struct timespec ts;
    struct timespec * timeout = nullptr;

    if (mode == WaitMode::Block)
    {
        if (timeout_ms != 0)
        {
            ts.tv_sec = timeout_ms / 1000;
            ts.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000;
            timeout = &ts;
        }
        // timeout_ms == 0 leaves `timeout` null, which is io_getevents' "wait forever" - matching
        // SharedQueue::pop and IoUringEngine.
    }
    else
    {
        // A zero timeout is what makes io_getevents return at once with whatever is ready. A null
        // pointer here would wait forever instead, which is the opposite of NonBlocking.
        //
        // It also skips the syscall when there is nothing to collect. libaio checks the ring in user
        // space first (io_getevents.c, aio_ring.h), and that check only applies when the timeout
        // pointer is non-null AND both its fields are zero - exactly this case. So an idle poll reads
        // two integers from the mapped ring and returns.
        ts.tv_sec = 0;
        ts.tv_nsec = 0;
        timeout = &ts;
    }

    const int ret = io_getevents(_ctx, min_nr, nr, _events.data(), timeout);
    if (ret < 0)
    {
        const int error = error_of(ret);

        // An interrupted wait only means nothing arrived, so it is answered as Success with no
        // completions. A wait that ran out of time never reaches this branch at all: io_getevents
        // reports a timeout by returning 0 events, not by failing.
        if (error != EINTR)
        {
            LOG(ERROR) << "io_getevents failed: " << std::strerror(error);
            return ResponseCode::UnknownError;
        }

        return ResponseCode::Success;
    }

    for (int i = 0; i < ret; ++i)
    {
        const struct io_event & event = _events[i];

        Completion & completion = out[out_count];
        completion.id = static_cast<RequestId>(reinterpret_cast<uintptr_t>(event.data));

        // io_event.res is declared UNSIGNED LONG (libaio.h, through PADDEDul), so on paper a failed
        // read arrives as a huge positive number. The cast is written out to say that the value is
        // read back as signed on purpose.
        //
        // It is not load-bearing, and a test cannot pin it: the implicit conversion keeps the same
        // bits, so -EINVAL comes out as -22 either way. Measured - removing the cast changed no test.
        // Kept for the reader, not for the compiler.
        //
        // Passed on as the kernel gave it: bytes when >= 0, minus an errno when < 0. A short read is
        // a small positive number, so it does not look like an error here. Mapping needs to know
        // whether the fd was direct, which this engine does not keep, so the caller maps.
        completion.res = static_cast<long>(event.res);

        // The iocb comes back through the event, so reclaiming is exact rather than a guess about
        // which request finished.
        struct iocb * iocb = event.obj;
        ASSERT(iocb >= _iocbs.data() && iocb < _iocbs.data() + _iocbs.size())
            << "io_getevents returned an iocb this engine does not own";
        _free.push_back(iocb);

        ++out_count;
    }

    return ResponseCode::Success;
}

}; // namespace runai::llm::streamer::common::posix_io
