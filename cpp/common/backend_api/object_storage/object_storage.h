#pragma once

#include "common/response/response.h"

namespace runai::llm::streamer::common::backend_api
{

using ObjectRequestId_t = uint64_t;

struct ObjectCompletionEvent_t
{
    ObjectRequestId_t request_id;     // ID provided by caller in obj_request_read
    ResponseCode response_code;
    size_t bytes_transferred;         // Actual bytes read into the caller's destination_buffer
};

} // namespace runai::llm::streamer::common::backend_api
