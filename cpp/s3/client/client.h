#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

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

    common::ResponseCode list(std::vector<std::string> & objects);

    common::ResponseCode bytesize(size_t * object_bytesize);

    // Stop sending requests to the object store
    // Requests that were already sent cannot be cancelled, since the Aws S3CrtClient does not support aborting requests
    // The S3CrtClient d'tor will wait for response of all teh sent requests, which can take a while
    void stop();

    std::string bucket() const;

    void path(const std::string & path);

 private:
    std::atomic<bool> _stop;
    ClientConfiguration _client_config;
    std::unique_ptr<Aws::S3Crt::S3CrtClient> _client;
    const Aws::String _bucket_name;
    Aws::String _path;

    // queue of asynchronous responses
    std::shared_ptr<common::Responder> _responder;
};

}; //namespace runai::llm::streamer::impl::s3
