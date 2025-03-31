
#include "common/response/response.h"

namespace runai::llm::streamer::common
{

Response::Response(unsigned file_index, unsigned index, common::ResponseCode ret) :
    file_index(file_index),
    index(index),
    ret(ret)
{}

Response::Response(unsigned index, ResponseCode ret) :
    Response(0, index, ret)
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
