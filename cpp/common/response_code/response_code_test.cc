#include "common/response_code/response_code.h"

#include <gtest/gtest.h>
#include <map>
#include <string>

#include "utils/logging/logging.h"
#include "utils/random/random.h"

namespace runai::llm::streamer::common
{

TEST(Response, Description)
{
    constexpr std::array<const char *, static_cast<size_t>(ResponseCode::__Max)> __strings = {
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

    // errors

    for (auto response_code : {ResponseCode::FileAccessError, ResponseCode::EofError, ResponseCode::InvalidParameterError, ResponseCode::EmptyRequestError, ResponseCode::BusyError, ResponseCode::UnknownError, ResponseCode::FinishedError, ResponseCode::S3NotSupported, ResponseCode::GlibcPrerequisite, ResponseCode::InsufficientFdLimit} )
    {
        std::string str = description(static_cast<int>(response_code));

        const auto expected = __strings[static_cast<int>(response_code)];

        EXPECT_EQ(str, expected);
    }
}

TEST(Description, Invalid)
{
    for (int response_code : { utils::random::number<int>(1000, 10000), utils::random::number<int>(-1000, -1) } )
    {
        std::string str = description(static_cast<int>(response_code));
        EXPECT_EQ(str, "Invalid response code");
    }
}

}; // namespace runai::llm::streamer::common
