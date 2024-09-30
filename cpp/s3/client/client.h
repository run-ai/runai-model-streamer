#pragma once

#include <memory>
#include <string>

#include "s3/client_configuration/client_configuration.h"

#include "common/storage_uri/storage_uri.h"
#include "common/responder/responder.h"
#include "common/response/response.h"
#include "common/range/range.h"

namespace runai::llm::streamer::impl::s3
{

struct S3Client
{
    S3Client(const common::s3::StorageUri & path);

    common::ResponseCode read(size_t offset, size_t bytesize, char * buffer);

    common::ResponseCode async_read(unsigned num_ranges, common::Range * ranges, size_t chunk_bytesize, char * buffer);

    common::Response async_read_response();

    std::string bucket() const;

    void path(const std::string & path);

 private:
    ClientConfiguration _client_config;
    std::unique_ptr<Aws::S3Crt::S3CrtClient> _client;
    const Aws::String _bucket_name;
    Aws::String _path;

    // queue of asynchronous responses
    std::shared_ptr<common::Responder> _responder;
};

}; //namespace runai::llm::streamer::impl::s3
