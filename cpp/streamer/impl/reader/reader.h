
#pragma once

#include <stddef.h>
#include <memory>
#include <vector>

#include "common/range/range.h"
#include "common/response/response.h"
#include "common/s3_wrapper/s3_wrapper.h"
namespace runai::llm::streamer::impl
{

struct Reader
{
    enum class Mode
    {
        Sync  = 0,
        Async = 1,
    };

    Reader(Mode mode) : mode(mode)
    {}

    virtual ~Reader() {}

    virtual void read(size_t bytesize, char * buffer) = 0;

    virtual void seek(size_t offset) = 0;

    // asynchronous

    virtual void async_read(const common::s3::S3ClientWrapper::Params & params, std::vector<common::Range> & ranges, char * buffer) = 0;
    virtual common::Response async_response() = 0;

    const Mode mode;
};

}; // namespace runai::llm::streamer::impl
