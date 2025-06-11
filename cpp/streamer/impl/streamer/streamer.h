
#pragma once

#include <memory>
#include <string>

#include "utils/threadpool/threadpool.h"
#include "utils/fdlimit/fdlimit.h"

#include "common/responder/responder.h"
#include "common/s3_credentials/s3_credentials.h"
#include "streamer/impl/config/config.h"
#include "streamer/impl/batch/batch.h"
#include "streamer/impl/reader/reader.h"
#include "streamer/impl/s3/s3.h"

namespace runai::llm::streamer::impl
{

// Streamer for reading large files concurrently, one file at a time

// Important Note:
// ==============
// The streamer can handle a single read request at a time
// The client must wait for the current request to end, before sending the next request

// Synchronous read -  read a range of a file to a given buffer of host memory
// Asynchronous read - read a range of a file to a given buffer of host memory in two stages:
//                          1. request to read a range, specifying a list of sub ranges
//                          2. wait for a response for the next ready sub range
//                     Responses are returned without any promissed order - a response is returned when a sub range is completed

struct Streamer
{
    Streamer();
    Streamer(Config config);
    ~Streamer();

    // single synchronous read request from offset in file
    // returns common::ResponseCode::Success if successful or error code
    common::ResponseCode request(const std::string & path, size_t offset, size_t bytesize, void * dst, const common::s3::Credentials & credentials);

    // async request to read a range asynchronously as multiple chunks
    // returns common::ResponseCode::Success if successful or error code
    common::ResponseCode request(const std::string & path, size_t offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes, const common::s3::Credentials & credentials);

    // return when there is a ready chunk
    // returns common::ResponseCode::FinishedError if no responses are expected
    // returns common::ResponseCode error if failed
    common::Response response();

    common::ResponseCode request_multi(
      std::vector<std::string> & paths,
      std::vector<size_t> & file_offsets,
      std::vector<size_t> & bytesizes,
      std::vector<void *> & dsts,
      std::vector<unsigned> & num_sizes,
      std::vector<size_t *> & internal_sizes,
      const common::s3::Credentials & credentials);
 
 private:
    void create_request(const std::string & path, size_t offset, size_t bytesize, void * dst, unsigned num_sizes, size_t * internal_sizes, const common::s3::Credentials & credentials);

    void Streamer::handle_request(unsigned file_index, const std::string & path, size_t file_offset, size_t bytesize, unsigned num_sizes, size_t * internal_sizes, void * dst, const common::s3::Credentials & credentials);

    void verify_requests(std::vector<std::string> & paths, std::vector<size_t> & file_offsets, std::vector<size_t> & bytesizes, std::vector<unsigned> & num_sizes, std::vector<void *> & dsts);

 private:
    std::shared_ptr<const Config> _config;
    std::unique_ptr<S3Cleanup> _s3;
    utils::ThreadPool<Batch> _pool;
    std::unique_ptr<S3Stop> _s3_stop;
    std::unique_ptr<utils::FdLimitSetter> _fd_limit;
    std::shared_ptr<common::Responder> _responder;
};

}; // namespace runai::llm::streamer::impl
