#include "common/posix_io/io_engine/io_engine.h"

#include <unistd.h>

#include <climits>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::posix_io
{

size_t max_read_bytesize(size_t page_size)
{
    // MAX_RW_COUNT = INT_MAX & PAGE_MASK (include/linux/fs.h).
    return static_cast<size_t>(INT_MAX) & ~(page_size - 1);
}

size_t max_read_bytesize()
{
    // No error check, on purpose - do not add one. sysconf(_SC_PAGESIZE) cannot fail: glibc reads it
    // from the auxiliary vector, musl uses a constant, and sysconf returns -1 only for an unknown
    // name. A fallback would be dead code that quietly used a different page size.
    return max_read_bytesize(static_cast<size_t>(::sysconf(_SC_PAGESIZE)));
}

std::unique_ptr<IoEngine> make_io_engine(Strategy strategy, const AsyncIoConfig & config)
{
    ASSERT(is_async(strategy)) << "Cannot build an engine for " << strategy
                               << " - it is served by the synchronous pool, not by an IoEngine";

    // No engine is implemented yet, so every strategy is unavailable - which is a state the
    // dispatcher must handle anyway (a blocked io_uring_setup produces it), so it can be built and
    // tested against this now.
    LOG(DEBUG) << "Read strategy " << strategy << " is not available: no engine is implemented yet"
               << " (depth " << config.depth << ", chunk " << config.chunk_bytesize << ")";

    return nullptr;
}

}; // namespace runai::llm::streamer::common::posix_io
