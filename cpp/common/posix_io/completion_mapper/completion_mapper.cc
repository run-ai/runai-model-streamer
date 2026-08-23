#include "common/posix_io/completion_mapper/completion_mapper.h"

#include <cerrno>
#include <sstream>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::common::posix_io
{

namespace
{

bool is_storage_error(long err)
{
    switch (err)
    {
    case EIO:
    case ENXIO:
    case ESTALE:
    case ETIMEDOUT:
    case ECONNRESET:
    case EREMOTEIO:
        return true;
    default:
        return false;
    }
}

} // namespace

bool is_internal_error(long res, const FileRef & file)
{
    if (res >= 0)
    {
        return false;
    }

    const long err = -res;

    // EFAULT means we handed the kernel a bad address; EBADF means the fd we passed was not open.
    // Neither can be the storage's doing - the engine never opens or closes anything, so an invalid fd
    // can only come from the caller's own bookkeeping.
    if (err == EFAULT || err == EBADF)
    {
        return true;
    }

    // EINVAL is diagnostic only on a DIRECT fd, where it means the alignment contract broke. On a
    // buffered fd it means something else entirely and belongs in the default branch.
    return err == EINVAL && file.direct;
}

ResponseCode map_completion(long res, const FileRef & file)
{
    if (res >= 0)
    {
        // Including res == 0. Whether that is EOF depends on what the caller asked for and how much
        // has already arrived, which only the caller knows - so it decides, not this.
        return ResponseCode::Success;
    }

    const long err = -res;

    if (err == ECANCELED)
    {
        return ResponseCode::FinishedError;
    }

    if (is_storage_error(err))
    {
        return ResponseCode::FileAccessError;
    }

    if (is_internal_error(res, file))
    {
        // Loud, but NOT fatal and NOT an exception.
        //
        // The design says "assert in debug, UnknownError in release". This repo's ASSERT is fatal in
        // every build - no NDEBUG guard, see logging.h - so using it here would throw out of
        // wait_for_completions, which is declared to return a code. A library inside a serving stack
        // must not take down its host either. So: log at ERROR and return.
        //
        // Never FileAccessError. That is the one outcome that sends an operator to investigate the
        // storage when the fault is ours.
        LOG(ERROR) << "Internal error: errno " << err << " on fd " << file.fd
                   << (file.direct ? " (direct)" : " (buffered)")
                   << " - an invariant of ours broke, not the storage";
        return ResponseCode::UnknownError;
    }

    LOG(ERROR) << "Unrecognised errno " << err << " from the io engine on fd " << file.fd;
    return ResponseCode::UnknownError;
}

}; // namespace runai::llm::streamer::common::posix_io
