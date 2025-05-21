#pragma once

#include "common/response/response.h"

namespace runai::llm::streamer::common
{

using ObjectRequestId_t = void*;

struct ObjectCompletionEvent_t
{
    ObjectRequestId_t request_id;     // ID provided by caller in obj_request_read
    ResponseCode status_code;         
    size_t bytes_transferred;         // Actual bytes read into the caller's destination_buffer
};

// TODO: add Path here

} // namespace runai::llm::streamer::common
