#pragma once

#include <cstddef>
#include <deque>
#include <map>
#include <vector>

#include "common/posix_io/io_engine.h"

namespace runai::llm::streamer::common::posix_io
{

// A test double for IoEngine: no kernel, no threads, and the test decides what completes and when.
//
// Its own Bazel package so //common/posix_io's source glob cannot reach it - glob() does not cross
// package boundaries - because a mock in the shipped library is a defect.
//
// Three things are otherwise reachable only against a real kernel under load, where they are
// non-deterministic: out-of-order completion, a partial flush(), and a wait that times out.
//
// NOT THREAD SAFE, like the interface it implements. Tests drive it from their own thread.
class MockIoEngine : public IoEngine
{
 public:
    explicit MockIoEngine(unsigned depth, Limits limits = Limits{});

    // What the engine was asked to do. Kept so a test can assert on the request itself: a chunk
    // staged at the wrong offset, or against the wrong destination, still completes cleanly.
    struct Request
    {
        RequestId id = 0;
        FileRef   file;
        size_t    offset = 0;
        size_t    bytesize = 0;
        char *    buffer = nullptr;
    };

    // ---- IoEngine ----

    Limits limits() const override;
    unsigned depth() const override;

    ResponseCode stage(RequestId id, FileRef file, size_t offset, size_t bytesize, char * buffer) override;

    // Issues up to the flush limit, oldest first. The rest stays staged - both real APIs issue a
    // prefix, so the unissued set is always the tail.
    ResponseCode flush(unsigned & out_issued) override;

    // Returns what the test has completed, in the order it completed it. Never blocks: with nothing
    // ready this is Success and zero for both wait modes, which is also the expired-timeout case - so
    // a test reaches the teardown path by simply completing nothing.
    ResponseCode wait_for_completions(Completion * out, unsigned max, unsigned & out_count,
                                      WaitMode mode, unsigned timeout_ms = 0) override;

    // Completes everything in flight as FinishedError, like io_uring's -ECANCELED. Staged requests
    // are left alone on purpose: no completion ever arrives for them, and discarding them is the
    // caller's teardown job.
    void cancel_all() override;

    void register_memory(char * base, size_t size) override;
    void unregister_memory(char * base) override;

    // ---- what the engine holds ----

    // Asserts if `id` is not live. Live means staged or in flight; completed requests are gone.
    const Request & request(RequestId id) const;

    std::vector<RequestId> staged() const;      // stage order
    std::vector<RequestId> in_flight() const;   // issue order

    size_t staged_count() const;
    size_t in_flight_count() const;

    // Every id ever staged, including completed ones - the live views empty out as completions
    // arrive, so this is what a test asserts the full set of requests against.
    const std::vector<Request> & history() const;

    // ---- injection ----

    // Issue at most `n` per flush; 0 means no limit. "Issue nothing" is set_flush_stalled(), so that
    // zero cannot mean two things.
    void set_flush_limit(unsigned n);

    // flush() issues nothing while set. Models io_submit's EAGAIN - including the RWF_NOWAIT kind,
    // which can fire with nothing in flight, so reaping frees nothing and a retry loop livelocks.
    void set_flush_stalled(bool stalled);

    // Make stage() / flush() return this code instead of acting.
    void set_stage_result(ResponseCode ret);
    void set_flush_result(ResponseCode ret);

    // ---- completion, in the order the test picks ----
    //
    // Each of these moves a request to the ready queue, and wait_for_completions() hands them out in
    // that order. Asserts if the request is not in flight - a kernel cannot complete something it was
    // never given, and allowing it would let a test assert an impossible state.

    void complete(RequestId id);                       // the full requested length
    void complete_short(RequestId id, size_t bytes);   // bytes < requested; 0 models EOF
    void fail(RequestId id, ResponseCode ret);
    void complete_all();                               // everything in flight, oldest first

    // ---- destination fill ----

    // When on (the default), a completion reporting N bytes writes N bytes of a pattern derived from
    // the file offset.
    //
    // On by default because a mock that transfers nothing cannot catch the mistakes that matter here:
    // a chunk staged against the wrong destination, or at the wrong offset, completes perfectly well.
    // Deriving the bytes from the offset makes both visible.
    void set_fill(bool fill);

    // The byte written for `file_offset`, so a test can build what it expects.
    static char pattern(size_t file_offset);

    // Fill `buffer` with the pattern for [offset, offset + bytesize).
    static void fill_expected(char * buffer, size_t offset, size_t bytesize);

 private:
    void ready(RequestId id, ResponseCode ret, size_t bytes_transferred);

    const unsigned _depth;
    const Limits _limits;

    std::map<RequestId, Request> _live;      // staged or in flight, by id
    std::deque<RequestId> _staged;           // stage order
    std::deque<RequestId> _in_flight;        // issue order
    std::deque<Completion> _ready;           // the order the test completed them

    std::vector<Request> _history;

    unsigned _flush_limit = 0;               // 0 = no limit
    bool _flush_stalled = false;
    bool _fill = true;

    ResponseCode _stage_result = ResponseCode::Success;
    ResponseCode _flush_result = ResponseCode::Success;
};

}; // namespace runai::llm::streamer::common::posix_io
