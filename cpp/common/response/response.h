
#pragma once

#include <ostream>

#include "common/response_code/response_code.h"

namespace runai::llm::streamer::common
{

struct Response
{
    // submission_id identifies the runai_request(_ex) submission this response belongs to,
    // so a shared responder can be demuxed across concurrent submissions. It is NOT the
    // internal impl::Request (a sub-range, addressed by file_index + index).
    Response(unsigned submission_id, unsigned file_index, unsigned index, common::ResponseCode ret);
    Response(unsigned file_index, unsigned index, common::ResponseCode ret);
    Response(unsigned index, common::ResponseCode ret);
    Response(unsigned index);
    Response(common::ResponseCode ret);


    bool operator==(const common::ResponseCode other);
    bool operator!=(const common::ResponseCode other);

    // id of the owning submission (one runai_request(_ex) call); 0 by default
    unsigned submission_id = 0;

    // index of file
    unsigned file_index;

    // index of sub request
    unsigned index;


    // response code
    common::ResponseCode ret;
};

std::ostream & operator<<(std::ostream & os, const Response & response);

}; // namespace runai::llm::streamer::common
