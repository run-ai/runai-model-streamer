
#pragma once

#include <ostream>

#include "common/response_code/response_code.h"

namespace runai::llm::streamer::common
{

struct Response
{
    Response(unsigned index, common::ResponseCode ret);
    Response(unsigned index);
    Response(common::ResponseCode ret);


    bool operator==(const common::ResponseCode other);
    bool operator!=(const common::ResponseCode other);

    // index of sub request
    unsigned index;

    // response code
    common::ResponseCode ret;
};

std::ostream & operator<<(std::ostream & os, const Response & response);

}; // namespace runai::llm::streamer::common
