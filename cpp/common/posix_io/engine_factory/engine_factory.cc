#include "common/posix_io/engine_factory/engine_factory.h"

#include <utility>

#include "common/exception/exception.h"
#include "common/posix_io/io_uring_engine/io_uring_engine.h"
#include "common/posix_io/io_uring_probe/io_uring_probe.h"
#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::posix_io
{

namespace
{

std::unique_ptr<IoEngine> make_io_uring_engine(Strategy strategy, const AsyncIoConfig & config)
{
    auto & probe = IoUringProbe::instance();

    // Consulted rather than re-derived. Both this and strategy resolution ask "can this host do
    // io_uring", and two mechanisms answering independently can disagree - resolution picking a
    // strategy this then refuses to build. One source of truth, cached, so the several engines built
    // under one-engine-per-mount cost one probe between them.
    const auto capability = probe.capability();
    if (!capability.available)
    {
        LOG(WARNING) << "Cannot build " << strategy << ": io_uring is unavailable here ("
                     << capability.error << ")";
        return nullptr;
    }

    try
    {
        return std::make_unique<IoUringEngine>(config);
    }
    catch (const common::Exception & e)
    {
        // The probe said io_uring works, and it does - a ring of THIS depth is what failed, on ENOMEM
        // or RLIMIT_MEMLOCK. Retrying cannot help, and leaving the strategy in the chain would have
        // resolution keep choosing it. Demote once, permanently.
        LOG(ERROR) << "io_uring is available but a ring of depth " << config.depth
                   << " could not be built: " << e.error();
        probe.mark_unavailable(e.error());
        return nullptr;
    }
}

} // namespace

std::unique_ptr<IoEngine> make_io_engine(Strategy strategy, const AsyncIoConfig & config)
{
    ASSERT(is_async(strategy)) << "Cannot build an engine for " << strategy
                               << " - it is served by the synchronous pool, not by an IoEngine";

    switch (strategy)
    {
    case Strategy::IoUringBuffered:
        return make_io_uring_engine(strategy, config);

    case Strategy::IoUringDirect:
        // The ring is the same; only the fds differ, and those belong to the caller. Waiting for the
        // O_DIRECT work rather than opening buffered under a direct name, which would read as working
        // while quietly not being direct at all.
        LOG(DEBUG) << strategy << " is not available: the direct path is not implemented yet";
        return nullptr;

    case Strategy::LibaioDirect:
        LOG(DEBUG) << strategy << " is not available: no libaio engine yet";
        return nullptr;

    default:
        return nullptr;
    }
}

}; // namespace runai::llm::streamer::common::posix_io
