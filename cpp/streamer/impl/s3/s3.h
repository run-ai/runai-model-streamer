#pragma once

#include <memory>
#include <vector>

#include "common/s3_wrapper/s3_wrapper.h"
#include "streamer/impl/reader/reader.h"
#include "streamer/impl/config/config.h"

namespace runai::llm::streamer::impl
{

struct S3Cleanup
{
    S3Cleanup() = default;

    ~S3Cleanup();
};

struct S3Stop
{
    S3Stop() = default;

    ~S3Stop();
};

struct S3 : Reader
{
    S3(std::shared_ptr<common::s3::S3ClientWrapper> client, const Config & config);
    virtual ~S3() {}

    void read(size_t bytesize, char * buffer) override;
    void seek(size_t offset) override;

    void async_read(std::vector<common::Range> & ranges, char * buffer) override;
    common::Response async_response() override;

 private:
    std::shared_ptr<common::s3::S3ClientWrapper> _client;
    const Config & _config;
    size_t _offset;
};

}; //namespace runai::llm::streamer::impl
