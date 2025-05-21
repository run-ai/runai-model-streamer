
#include "s3/response/response.h"

namespace runai::llm::streamer::impl::s3
{

Response::Response(common::ObjectRequestId_t request_id, common::ResponseCode ret) :
    request_id(request_id),
    ret(ret)
{}

Response::Response(common::ObjectRequestId_t request_id) :
    Response(request_id, common::ResponseCode::Success)
{}


bool Response::operator==(const common::ResponseCode other)
{
    return ret == other;
}

bool Response::operator!=(const common::ResponseCode other)
{
    return ret != other;
}

std::ostream & operator<<(std::ostream & os, const Response & response)
{
    return os << "Request id: " << response.request_id << " Response code: " << response.ret;
}

}; // namespace runai::llm::streamer::common
