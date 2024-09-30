
#include "common/response/response.h"

namespace runai::llm::streamer::common
{

Response::Response(unsigned index, ResponseCode ret) :
    index(index),
    ret(ret)
{}

Response::Response(unsigned index) : Response(index, ResponseCode::Success)
{}

Response::Response(ResponseCode ret) :
    ret(ret)
{}

bool Response::operator==(const ResponseCode other)
{
    return ret == other;
}

bool Response::operator!=(const ResponseCode other)
{
    return ret != other;
}

std::ostream & operator<<(std::ostream & os, const Response & response)
{
    return os << "request index: " << response.index << " Response code: " << response.ret;
}

}; // namespace runai::llm::streamer::common
