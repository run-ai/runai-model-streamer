
#pragma once

#include <ostream>

namespace runai::llm::streamer::common
{

enum class ResponseCode : int
{
    Success           = 0,

    FinishedError,
    FileAccessError,
    EofError,
    S3NotSupported,
    GlibcPrerequisite,
    InsufficientFdLimit,
    InvalidParameterError,
    EmptyRequestError,
    BusyError,
    CaFileNotFound,
    UnknownError,
    ObjPluginLoadError,
    GCSNotSupported,
    AzureBlobNotSupported,
    FileTruncatedError,
    TimedOut,
    UnsupportedBackendMix,
    CredentialsAlreadySet,
    RetryableFileAccessError,

    // Filesystem strategy problems. Two codes, because the operator has to do something different
    // for each one: set the value once, or add a candidate the host can serve.
    //
    // Both used to report UnsupportedBackendMix, whose message is about mixing S3, GCS and Azure.
    // That sent the reader to object storage for a problem that has nothing to do with it.
    //
    // APPENDED here, before __Max, so the numbers of the codes above do not move. They cross the C
    // ABI, and a compiled caller holds the old numbers. RetryableFileAccessError is already released
    // and keeps its number, so these two follow it rather than displacing it.
    FsStrategyConflict,
    FsStrategyUnavailable,

    // One mount's asynchronous reader failed permanently, mid-run - io_uring_submit or io_getevents
    // returned an error that is not backpressure.
    //
    // NOT UnknownError. That code means the failure is ours and nothing we report can be trusted, so a
    // caller seeing it should abort everything and treat it as a bug in the streamer. None of that
    // applies here: every other mount has its own engine, object storage is a different pool, and this
    // mount is still readable by the synchronous reader.
    //
    // NOT FileAccessError either. The storage is healthy and the ring is not, and that code sends an
    // operator to look at the wrong thing - the same mistake the two codes above were added to correct.
    //
    // What a caller does with it: these ranges were not read, and asking for them again succeeds,
    // because the engine is not reused.
    //
    // ONE code for both engines. The decision it drives is the same whichever one failed; which engine
    // it was, and with what errno, is in the log.
    FsAsyncEngineError,

    __Max,
};

const char * description(int response_code);

ResponseCode response_code_from(int value);

std::ostream & operator<<(std::ostream &, const ResponseCode &);

}; // namespace runai::llm::streamer::common
