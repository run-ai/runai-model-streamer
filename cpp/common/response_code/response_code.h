
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
    // Internal object-storage completion: the backend exhausted its own retry policy, but classified
    // the terminal failure as safe for the streamer to retry. ObjectStorageWorker consumes this code and
    // must convert it to FileAccessError before a response reaches the public API.
    RetryableFileAccessError,
    __Max,
};

const char * description(int response_code);

ResponseCode response_code_from(int value);

std::ostream & operator<<(std::ostream &, const ResponseCode &);

}; // namespace runai::llm::streamer::common
