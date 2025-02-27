#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/range/range.h"
#include "common/response_code/response_code.h"
#include "common/storage_uri/storage_uri.h"
#include "common/response/response.h"

#include "utils/dylib/dylib.h"
#include "utils/semver/semver.h"

namespace runai::llm::streamer::common::s3
{

struct S3ClientWrapper
{
    S3ClientWrapper(const StorageUri & uri);
    ~S3ClientWrapper();

    // request to read a continous range into a buffer
    // the range is divided into sub ranges, which will generate response whenever a full sub range is fully read
    // ranges - list of sub ranges
    // chunk_bytesize - size of chunk for reading in multi parts (minimal size is 5 MB)

    ResponseCode async_read(std::vector<Range>& ranges, size_t chunk_bytesize, char * buffer);
    Response async_read_response();

    // stop - stops the responder of each S3 client, in order to notify callers which sent a request and are waiting for a response
    //        required for stopping the threadpool workers, which are bloking on the client responder
    static void stop();

    // destroy S3 all clients
    static void shutdown();

    static constexpr size_t min_chunk_bytesize = 5 * 1024 * 1024;
    static constexpr size_t default_chunk_bytesize = 8 * 1024 * 1024;

 private:
    void * create_client(const StorageUri & uri);
    static std::shared_ptr<utils::Dylib> open_s3();
    static std::shared_ptr<utils::Dylib> open_s3_impl();

 private:
    std::shared_ptr<utils::Dylib> _s3_dylib;

    // Handle to s3 client
    void * _s3_client;
};

}; //namespace runai::llm::streamer::common::s3
