#pragma once

#include <cstddef>
#include <string>

#include "common/posix_io/io_engine/io_engine.h"
#include "common/response_code/response_code.h"

namespace runai::llm::streamer::common::posix_io
{

// What one read asked for. Two of the mappings below need it, so it travels with the result rather
// than being looked up.
struct Requested
{
    size_t offset = 0;
    size_t bytesize = 0;
    const char * buffer = nullptr;
};

// Turn an engine result into a ResponseCode.
//
// `res` is what the kernel reported for one read: bytes transferred when >= 0, a negative errno
// otherwise. io_uring and libaio both report this way, and both use this - one table, because the
// promise that "one file's error does not poison other submissions" has to hold identically for
// either engine, and two copies of it would drift apart unnoticed.
//
// Lives beside the engines, not above them: Completion::ret is a ResponseCode, so mapping is part of
// being an engine. This is the same layering object storage has, where each plugin maps its own SDK's
// errors behind the C ABI and the worker only ever sees a ResponseCode.
//
//   res >= 0                            Success - the CALLER still compares res against bytesize, and
//                                       re-stages the remainder if it is short. A short read is not an
//                                       error, and only the caller knows what it asked for.
//   -EIO -ENXIO -ESTALE -ETIMEDOUT      FileAccessError - this file failed; others carry on
//   -ECONNRESET -EREMOTEIO
//   -ECANCELED                          FinishedError - teardown, NOT a storage fault. Reporting it as
//                                       one sends an operator to investigate the wrong system.
//   -EINVAL on a direct fd              our bug: the alignment contract broke. See below.
//   -EFAULT -EBADF                      our bug
//   anything else                       UnknownError, with the raw errno logged
//
// No -EINTR row on purpose. It is unreachable as a completion: regular-file reads sit in
// uninterruptible sleep, io-wq workers do not take signals the usual way, and libaio surfaces EINTR
// from io_getevents - the wait, not the result. The default branch covers it if it ever appears, which
// beats an untested branch claiming to handle it.
ResponseCode map_completion(long res, const FileRef & file);

// Why a direct read was rejected, naming which constraint broke:
//
//   direct read rejected: offset=8000 (align 512), buffer=0x... (align 512), length=100 (align 512), congruent=0
//
// -EINVAL on a direct fd means we broke our own alignment rule, so the engine should say which part
// rather than emit a bare EINVAL and send someone to look at the filesystem. Recomputed from the
// request, which is why the mapper is given it.
std::string alignment_diagnosis(const Requested & requested, const Limits & limits);

// True when a result means "our bug" rather than "the storage failed".
//
// map_completion logs these at ERROR and returns UnknownError. It does not assert and does not throw:
// this repo's ASSERT is fatal in EVERY build, and throwing here would break wait_for_completions,
// which returns a code. Never FileAccessError - that sends an operator to the wrong system.
bool is_internal_error(long res, const FileRef & file);

}; // namespace runai::llm::streamer::common::posix_io
