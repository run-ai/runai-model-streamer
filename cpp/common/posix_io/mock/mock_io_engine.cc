#include "common/posix_io/mock/mock_io_engine.h"

#include <algorithm>
#include <cstdint>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::posix_io
{

namespace
{

// Linear, which is fine: these hold at most `depth` entries, and this is a test double.
bool erase_from(std::deque<RequestId> & queue, RequestId id)
{
    const auto it = std::find(queue.begin(), queue.end(), id);
    if (it == queue.end())
    {
        return false;
    }
    queue.erase(it);
    return true;
}

// A test that does not care about the cap gets the real kernel one. Here rather than in the
// constructor body because _limits is const - an engine's limits do not change after it is built.
Limits with_default_cap(Limits limits)
{
    if (limits.max_read_bytesize == 0)
    {
        limits.max_read_bytesize = max_read_bytesize();
    }
    return limits;
}

} // namespace

MockIoEngine::MockIoEngine(unsigned depth, Limits limits) :
    _depth(depth),
    _limits(with_default_cap(limits))
{
}

Limits MockIoEngine::limits() const
{
    return _limits;
}

unsigned MockIoEngine::depth() const
{
    return _depth;
}

ResponseCode MockIoEngine::stage(RequestId id, FileRef file, size_t offset, size_t bytesize, char * buffer)
{
    if (_stage_result != ResponseCode::Success)
    {
        // Nothing is recorded: no completion will arrive, and the caller must resolve it itself.
        return _stage_result;
    }

    // The id is the slot index, so staging onto a live slot means the caller lost its free list. A
    // real engine would then send two completions with the same id, and the second would route to
    // whatever took over the slot.
    ASSERT(_live.count(id) == 0) << "id " << id << " is already live";
    ASSERT(id < _depth) << "id " << id << " is outside the window of " << _depth;

    const Request request{ id, file, offset, bytesize, buffer };

    _live[id] = request;
    _staged.push_back(id);
    _history.push_back(request);

    return ResponseCode::Success;
}

ResponseCode MockIoEngine::flush(unsigned & out_issued)
{
    out_issued = 0;

    if (_flush_result != ResponseCode::Success)
    {
        return _flush_result;
    }

    if (_flush_stalled)
    {
        // Zero progress is backpressure, not an error: the caller must keep calling flush().
        return ResponseCode::Success;
    }

    const size_t limit = (_flush_limit == 0) ? _staged.size() : std::min<size_t>(_flush_limit, _staged.size());

    // A prefix, oldest first, as both real APIs do - so the unissued set is always the tail and
    // nothing has to record which requests failed to issue.
    for (size_t i = 0; i < limit; ++i)
    {
        _in_flight.push_back(_staged.front());
        _staged.pop_front();
        ++out_issued;
    }

    return ResponseCode::Success;
}

ResponseCode MockIoEngine::wait_for_completions(Completion * out, unsigned max, unsigned & out_count,
                                                WaitMode mode, unsigned timeout_ms)
{
    // Before anything is harvested: a wait is when the kernel hands over what it has finished.
    for (unsigned i = 0; i < _auto_complete_on_wait && !_in_flight.empty(); ++i)
    {
        const auto id = _in_flight.front();
        ready(id, ResponseCode::Success, _live.at(id).bytesize);
    }

    // Neither is acted on: this never blocks, so both wait modes return whatever is ready. An expired
    // timeout is Success with zero completions anyway, so a test gets that case for free - no clock,
    // no flakiness. They are RECORDED, because whether the caller blocks with nothing issued is
    // exactly the thing worth asserting.
    _last_wait_mode = mode;
    _last_wait_timeout_ms = timeout_ms;
    ++_waits;

    out_count = 0;

    while (out_count < max && !_ready.empty())
    {
        out[out_count] = _ready.front();
        _ready.pop_front();
        ++out_count;
    }

    return ResponseCode::Success;
}

void MockIoEngine::register_memory(char * base, size_t size)
{
    LOG(SPAM) << "Mock engine registering " << size << " bytes at " << static_cast<void *>(base);
}

void MockIoEngine::unregister_memory(char * base)
{
    LOG(SPAM) << "Mock engine unregistering " << static_cast<void *>(base);
}

const MockIoEngine::Request & MockIoEngine::request(RequestId id) const
{
    const auto it = _live.find(id);
    ASSERT(it != _live.end()) << "id " << id << " is not live";
    return it->second;
}

std::vector<RequestId> MockIoEngine::staged() const
{
    return std::vector<RequestId>(_staged.begin(), _staged.end());
}

std::vector<RequestId> MockIoEngine::in_flight() const
{
    return std::vector<RequestId>(_in_flight.begin(), _in_flight.end());
}

size_t MockIoEngine::staged_count() const
{
    return _staged.size();
}

size_t MockIoEngine::in_flight_count() const
{
    return _in_flight.size();
}

const std::vector<MockIoEngine::Request> & MockIoEngine::history() const
{
    return _history;
}

void MockIoEngine::set_auto_complete_on_wait(unsigned per_wait)
{
    _auto_complete_on_wait = per_wait;
}

void MockIoEngine::set_flush_limit(unsigned n)
{
    _flush_limit = n;
}

void MockIoEngine::set_flush_stalled(bool stalled)
{
    _flush_stalled = stalled;
}

void MockIoEngine::set_stage_result(ResponseCode ret)
{
    _stage_result = ret;
}

void MockIoEngine::set_flush_result(ResponseCode ret)
{
    _flush_result = ret;
}

void MockIoEngine::complete(RequestId id)
{
    const auto it = _live.find(id);
    ASSERT(it != _live.end()) << "id " << id << " is not live";

    ready(id, ResponseCode::Success, it->second.bytesize);
}

void MockIoEngine::complete_short(RequestId id, size_t bytes)
{
    const auto it = _live.find(id);
    ASSERT(it != _live.end()) << "id " << id << " is not live";
    ASSERT(bytes < it->second.bytesize) << "short completion of " << bytes
                                        << " is not shorter than the requested " << it->second.bytesize;

    // Still Success: a short count is not an error, and is visible only by comparing the count
    // against what was asked for.
    ready(id, ResponseCode::Success, bytes);
}

void MockIoEngine::fail(RequestId id, ResponseCode ret)
{
    const auto it = _live.find(id);
    ASSERT(it != _live.end()) << "id " << id << " is not live";

    ready(id, ret, 0);
}

void MockIoEngine::complete_all()
{
    while (!_in_flight.empty())
    {
        complete(_in_flight.front());
    }
}

void MockIoEngine::set_fill(bool fill)
{
    _fill = fill;
}

WaitMode MockIoEngine::last_wait_mode() const
{
    return _last_wait_mode;
}

unsigned MockIoEngine::last_wait_timeout_ms() const
{
    return _last_wait_timeout_ms;
}

unsigned MockIoEngine::waits() const
{
    return _waits;
}

char MockIoEngine::pattern(size_t file_offset)
{
    // From the absolute file offset, so bytes landing in the wrong place - or the right place filled
    // from the wrong offset - do not compare equal.
    //
    // Multiply by the odd golden-ratio constant and take the HIGH byte, so every input bit reaches
    // the result. Do NOT take the low byte of a product instead: (x * k) & 0xFF depends only on
    // x & 0xFF, so bits above 7 vanish. That version repeated exactly every 64 KiB - one of the
    // strides a wrong destination is most likely to be off by - and the test caught it.
    const uint64_t mixed = static_cast<uint64_t>(file_offset) * 0x9E3779B97F4A7C15ull;
    return static_cast<char>((mixed >> 56) & 0xFF);
}

void MockIoEngine::fill_expected(char * buffer, size_t offset, size_t bytesize)
{
    for (size_t i = 0; i < bytesize; ++i)
    {
        buffer[i] = pattern(offset + i);
    }
}

void MockIoEngine::ready(RequestId id, ResponseCode ret, size_t bytes_transferred)
{
    const auto it = _live.find(id);
    ASSERT(it != _live.end()) << "id " << id << " is not live";

    const auto & request = it->second;

    // Only issued requests can complete. Allowing a staged one would let a test assert a state no
    // kernel can produce, and would blur the staged/in-flight split the teardown path needs.
    ASSERT(erase_from(_in_flight, id)) << "id " << id << " has not been issued - flush() first";

    if (_fill && bytes_transferred > 0 && request.buffer != nullptr)
    {
        // Exactly what was reported, never what was requested - or a short completion would hide a
        // caller that trusts the return code over the byte count.
        fill_expected(request.buffer, request.offset, bytes_transferred);
    }

    _ready.push_back(Completion{ id, ret, bytes_transferred });
    _live.erase(it);
}

}; // namespace runai::llm::streamer::common::posix_io
