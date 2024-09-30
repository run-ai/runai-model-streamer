#include "common/range/range.h"

namespace runai::llm::streamer::common
{

Range::Range(size_t start, size_t size) :
    start(start),
    size(size)
{}

std::ostream & operator<<(std::ostream & os, const Range & r)
{
    return os << "offset : " << r.start << " bytesize: " << r.size;
}

}; //namespace runai::llm::streamer::common
