#pragma once

#include <ostream>

namespace runai::llm::streamer::common
{

struct Range
{
    Range() = default;
    Range(size_t start, size_t size);
    size_t start;
    size_t size;
};

std::ostream & operator<<(std::ostream & os, const Range & r);

}; //namespace runai::llm::streamer::common
