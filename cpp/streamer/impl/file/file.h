
#pragma once

#include <string>
#include <vector>

#include "streamer/impl/reader/reader.h"
#include "streamer/impl/config/config.h"

#include "utils/fd/fd.h"

namespace runai::llm::streamer::impl
{

struct File : Reader
{
    File(const std::string & path, const Config & config);
    virtual ~File() {}

    void read(size_t bytesize, char * buffer) override;

    void seek(size_t offset) override;

    void async_read(std::vector<common::Range> & ranges, char * buffer) override;
    common::Response async_response() override;

 private:
    utils::Fd _fd;
    size_t _block_size;
};

}; // namespace runai::llm::streamer::impl
