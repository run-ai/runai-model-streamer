#include "posix_io/io_uring_probe/io_uring_probe.h"

#include <liburing.h>

#include <cerrno>
#include <cstring>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::posix_io
{

namespace
{

// Availability does not depend on depth, so probe with the smallest useful ring. The engine asks for
// the configured depth and reports its own failure.
constexpr unsigned ProbeEntries = 8;

// EPERM and EACCES mean "this kernel has io_uring but this process may not use it" - seccomp, or
// kernel.io_uring_disabled. Everything else (ENOSYS above all) means it is simply not here.
common::ResponseCode reason_for(int error)
{
    return (error == EPERM || error == EACCES) ? common::ResponseCode::FileAccessError : common::ResponseCode::UnknownError;
}

} // namespace

IoUringCapability probe_io_uring()
{
    IoUringCapability capability;

    // liburing returns -errno rather than setting errno.
    struct io_uring ring;
    const int ret = io_uring_queue_init(ProbeEntries, &ring, 0);
    if (ret < 0)
    {
        capability.error = reason_for(-ret);
        LOG(WARNING) << "io_uring is not available: io_uring_setup failed: " << std::strerror(-ret)
                     << ". Checking the kernel version instead would have said otherwise - a seccomp"
                     << " profile or kernel.io_uring_disabled blocks the syscall on kernels that"
                     << " support it";
        return capability;
    }

    capability.timed_wait_is_free = (ring.features & IORING_FEAT_EXT_ARG) != 0;

    // Probe against the ring we already have. io_uring_get_probe() would set up and tear down a
    // SECOND ring to answer the same question.
    struct io_uring_probe * probe = io_uring_get_probe_ring(&ring);
    const bool op_read = (probe != nullptr) && io_uring_opcode_supported(probe, IORING_OP_READ);
    if (probe != nullptr)
    {
        io_uring_free_probe(probe);
    }

    io_uring_queue_exit(&ring);

    if (!op_read)
    {
        // A ring we cannot read through is of no use to us. Reachable only below 5.6, well under our
        // floor - but the rule is to probe rather than to trust the version.
        capability.error = common::ResponseCode::UnknownError;
        LOG(WARNING) << "io_uring is not available: a ring was created but IORING_OP_READ is not"
                     << " supported";
        return capability;
    }

    capability.available = true;

    LOG(INFO) << "io_uring is available"
              << (capability.timed_wait_is_free ? "" : " (without IORING_FEAT_EXT_ARG: a bounded wait"
                                                       " costs a submission slot)");
    return capability;
}

IoUringCapability IoUringProbe::capability()
{
    std::unique_lock<std::mutex> lock(_mutex);

    if (!_probed)
    {
        _capability = probe_io_uring();
        _probed = true;
    }

    return _capability;
}

void IoUringProbe::mark_unavailable(common::ResponseCode reason)
{
    std::unique_lock<std::mutex> lock(_mutex);

    // Already out of the chain, so keep the first reason - it says what actually went wrong, while
    // later failures are consequences of already being disabled.
    //
    // The _probed test is not redundant: an un-probed capability is ALSO !available, since that is
    // how the struct default-constructs. Without it the first demotion on a host where the probe has
    // not run yet would be read as "already unavailable" and its reason thrown away.
    if (_probed && !_capability.available)
    {
        return;
    }

    // Marks it probed even if it never was. Reaching here normally means an engine was built, so the
    // probe has run - but recording it either way stops a later capability() from probing afresh and
    // cheerfully answering "available" after a real failure.
    _probed = true;
    _capability.available = false;
    _capability.error = reason;

    LOG(WARNING) << "Disabling io_uring for the rest of this process: " << reason
                 << ". The probe succeeded, so the host supports io_uring - building an engine at the"
                 << " configured depth is what failed, and it will not succeed later";
}

IoUringProbe & IoUringProbe::instance()
{
    static IoUringProbe probe;
    return probe;
}

}; // namespace runai::llm::streamer::posix_io
