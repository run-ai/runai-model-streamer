
#include "common/response_code/response_code.h"

#include <array>
#include <string>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::common
{

const char * response_invalid = "Invalid response code";

ResponseCode response_code_from(int value)
{
    if (value < 0 || value >= static_cast<int>(ResponseCode::__Max))
    {
        LOG(ERROR) << "Value " << value << " is not a valid response code";
        throw;
    }

    return static_cast<ResponseCode>(value);
}

constexpr std::array<const char *, static_cast<size_t>(ResponseCode::__Max)> __messages = {
    "Request sent successfuly",
    "Finished all responses",
    "File access error",
    "End of file reached",
    "S3 not supported",
    "GLIBC version should be at least 2.29",
    "Increase process fd limit or decrease the concurrency level. Recommended value for the streamer alone is the concurrency multiplied by 64, in addition to your application fd usage",
    "Invalid request parameters",
    "Empty request parameters",
    "Streamer is handling previous request",
    "Unknown Error"
};

const char * description(int response_code)
{
    if (response_code < 0 || response_code >= static_cast<int>(ResponseCode::__Max))
    {
        return response_invalid;
    }

    return __messages[response_code];
}

std::ostream & operator<<(std::ostream & os, const ResponseCode & ret)
{
    return os << " response code: " << description(static_cast<int>(ret));
}

}; // namespace runai::llm::streamer::common
