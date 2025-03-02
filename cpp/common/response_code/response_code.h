
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
    UnknownError,
    __Max,
};

const char * description(int response_code);

ResponseCode response_code_from(int value);

std::ostream & operator<<(std::ostream &, const ResponseCode &);

}; // namespace runai::llm::streamer::common
