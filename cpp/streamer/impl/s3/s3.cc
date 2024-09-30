#include <memory>
#include <regex>
#include <vector>

#include "common/exception/exception.h"
#include "streamer/impl/s3/s3.h"

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl
{

S3::S3(std::shared_ptr<common::s3::S3ClientWrapper> client, const Config & config) :
    Reader(Reader::Mode::Async),
    _client(client),
    _config(config)
{
}

void S3::seek(size_t offset)
{
    LOG(ERROR) << "Not implemented";
    throw common::Exception(common::ResponseCode::UnknownError);
}

void S3::read(size_t bytesize, char * buffer)
{
    LOG(ERROR) << "Not implemented";
    throw common::Exception(common::ResponseCode::UnknownError);
}

void S3::async_read(std::vector<common::Range> & ranges, char * buffer)
{
    auto response_code = _client->async_read(ranges, _config.s3_block_bytesize, buffer);
    if (response_code != common::ResponseCode::Success)
    {
        throw common::Exception(response_code);
    }
}

common::Response S3::async_response()
{
    return _client->async_read_response();
}

S3Cleanup::~S3Cleanup()
{
    try
    {
        common::s3::S3ClientWrapper::shutdown();
    }
    catch(...)
    {
    }
}

}; // namespace runai::llm::streamer::impl
