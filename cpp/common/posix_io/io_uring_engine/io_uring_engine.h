#pragma once

#include <liburing.h>

#include <cstddef>
#include <memory>

#include "common/posix_io/io_engine/io_engine.h"

namespace runai::llm::streamer::common::posix_io
{

// IoEngine over io_uring.
//
// NOT THREAD SAFE, like the interface. One worker owns it and makes every call - which is what keeps
// this free of locks and what the ring's user-space head/tail state needs anyway. The SQ and CQ sides
// *can* be split across two threads (io_uring supports it), but our state cannot: see design 5.2.4
// for why that split is rejected.
//
// Deliberately absent:
//
//   IORING_SETUP_SQPOLL   a busy-polling kernel thread per ring, for little gain once submissions are
//                         batched by stage/flush (5.7)
//   IORING_SETUP_CLAMP    the one flag that could hand back a ring SMALLER than asked for, which is
//                         the only way the window could exceed the queue (5.7). Without it the kernel
//                         rounds up or refuses.
//   fixed files           ~64 us/s saved at our request rate, against real machinery (5.4)
//   registered buffers    destinations belong to the caller, and the Python ring frees them out from
//                         under us (5.10)
//   cancellation          teardown is quiesce-then-report in the worker; see io_engine.h
class IoUringEngine : public IoEngine
{
 public:
    // Builds the ring, or throws common::Exception if this host cannot. Callers should go through
    // make_io_engine(), which consults IoUringProbe first and turns a failure into nullptr.
    IoUringEngine(const AsyncIoConfig & config, size_t max_read_bytesize);
    explicit IoUringEngine(const AsyncIoConfig & config);

    ~IoUringEngine() override;

    Limits limits() const override;

    // The ring's REAL size, which is not always what was asked for: io_uring rounds entries up to a
    // power of two. The caller's window is sized from this, never from AsyncIoConfig::depth, so the
    // window can never exceed the queue.
    unsigned depth() const override;

    ResponseCode stage(RequestId id, FileRef file, size_t offset, size_t bytesize, char * buffer) override;
    ResponseCode flush(unsigned & out_issued) override;
    ResponseCode wait_for_completions(Completion * out, unsigned max, unsigned & out_count,
                                      WaitMode mode, unsigned timeout_ms = 0) override;

 private:
    // Prepared with io_uring_get_sqe() and not yet handed to io_uring_submit(). The kernel has not
    // seen these; only flush() makes them real.
    unsigned _staged = 0;

    struct io_uring _ring;
    Limits _limits;
    unsigned _depth = 0;
};

}; // namespace runai::llm::streamer::common::posix_io
