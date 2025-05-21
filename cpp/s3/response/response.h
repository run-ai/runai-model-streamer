
#pragma once

#include <ostream>

#include "common/response_code/response_code.h"
#include "common/backend_api/object_storage/object_storage.h"

namespace runai::llm::streamer::impl::s3
{

struct Response
{
    Response(common::ObjectRequestId_t request_id, common::ResponseCode ret);
    Response(common::ObjectRequestId_t request_id);

    bool operator==(const common::ResponseCode other);
    bool operator!=(const common::ResponseCode other);

    // request id
    common::ObjectRequestId_t request_id;

    // response code
    common::ResponseCode ret;
};

std::ostream & operator<<(std::ostream & os, const Response & response);

}; // namespace runai::llm::streamer::impl::s3
