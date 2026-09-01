
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

    __Max,
};

const char * description(int response_code);

ResponseCode response_code_from(int value);

std::ostream & operator<<(std::ostream &, const ResponseCode &);

}; // namespace runai::llm::streamer::common
