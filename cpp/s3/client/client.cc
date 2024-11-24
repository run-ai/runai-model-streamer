
#include <aws/s3-crt/model/GetObjectRequest.h>

#include <algorithm>
#include <string>
#include <utility>

#include "s3/client/client.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::s3
{

S3Client::S3Client(const common::s3::StorageUri & uri) :
    _stop(false),
    _bucket_name(uri.bucket.c_str(), uri.bucket.size()),
    _path(uri.path.c_str(), uri.path.size())
{
    if (!uri.endpoint.empty())
    {
        _client_config.config.endpointOverride = Aws::String(uri.endpoint.c_str(), uri.endpoint.size());
    }

    if (utils::try_getenv("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING", _client_config.config.useVirtualAddressing))
    {
        LOG(DEBUG) << "Setting s3 configuration useVirtualAddressing to " << _client_config.config.useVirtualAddressing;
    }

    _client = std::make_unique<Aws::S3Crt::S3CrtClient>(_client_config.config);
}

common::ResponseCode S3Client::read(size_t offset, size_t bytesize, char * buffer)
{
    std::string range_str = "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + bytesize);

    Aws::S3Crt::Model::GetObjectRequest request;
    request.SetBucket(_bucket_name);
    request.SetKey(_path);
    request.SetRange(range_str.c_str());

    request.SetResponseStreamFactory(
        [buffer, bytesize]()
        {
            std::unique_ptr<Aws::StringStream>
                    stream(Aws::New<Aws::StringStream>("RunaiBuffer"));

            stream->rdbuf()->pubsetbuf(buffer, bytesize);

            return stream.release();
        });

    Aws::S3Crt::Model::GetObjectOutcome outcome = _client->GetObject(request);

    if (!outcome.IsSuccess()) {
        const auto & err = outcome.GetError();
        LOG(ERROR) << "Failed to download s3 object " << err.GetExceptionName() << ": " << err.GetMessage();
        return common::ResponseCode::FileAccessError;
    }

    LOG(SPAM) << "Successfully retrieved '" << _path << "' from '" << _bucket_name << "'."  << range_str;
    return common::ResponseCode::Success;
}

// returns response object that contains the index of the range in ranges vector  which was passed in the request (0... number of ranges - 1)
common::Response S3Client::async_read_response()
{
    if (_responder == nullptr)
    {
        LOG(WARNING) << "Requesting response with uninitialized responder";
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

// aynchronously read consecutive ranges, producing a Response object per range in the ranges vector
common::ResponseCode S3Client::async_read(unsigned num_ranges, common::Range * ranges, size_t chunk_bytesize, char * buffer)
{
    if (_responder != nullptr && !_responder->finished())
    {
        LOG(ERROR) << "S3 client has not finished the previous async request";
        return common::ResponseCode::BusyError;
    }

    _responder = std::make_shared<common::Responder>(num_ranges);

    Aws::S3Crt::Model::GetObjectRequest request;
    request.SetBucket(_bucket_name);
    request.SetKey(_path);

    char * buffer_ = buffer;
    common::Range * ranges_ = ranges;
    for (unsigned ir = 0; ir < num_ranges && !_stop; ++ir)
    {
        const auto & range_ = *ranges_;

        // split range into chunks
        size_t size = std::max(1UL, range_.size/chunk_bytesize);
        LOG(DEBUG) <<"Number of chunks is " << size;

        // each range is divided into chunks (size is the number of chunks)
        // when all the chunks have been read successfuly the response for that range is pushed to the responder
        auto counter = std::make_shared< std::atomic<unsigned> >(size);
        // success flag for the current range is passed to the client
        auto is_success = std::make_shared< std::atomic<bool> >(true);

        size_t total_ = range_.size;
        size_t offset_ = range_.start;
        for (unsigned i = 0; i < size && !_stop; ++i)
        {
            size_t bytesize_ = (i == size - 1 ? total_ : chunk_bytesize);

            // send async request

            std::string range_str = "bytes=" + std::to_string(offset_) + "-" + std::to_string(offset_ + bytesize_);
            request.SetRange(range_str.c_str());

            request.SetResponseStreamFactory(
                [buffer_, bytesize_]()
                {
                    std::unique_ptr<Aws::StringStream>
                            stream(Aws::New<Aws::StringStream>("RunaiBuffer"));

                    stream->rdbuf()->pubsetbuf(buffer_, bytesize_);

                    return stream.release();
                });

            _client->GetObjectAsync(request, [responder = _responder, ir, counter, is_success](const Aws::S3Crt::S3CrtClient*, const Aws::S3Crt::Model::GetObjectRequest&,
                                                                            const Aws::S3Crt::Model::GetObjectOutcome& outcome,
                                                                            const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
                if (outcome.IsSuccess())
                {
                    const auto running = counter->fetch_sub(1);
                    LOG(SPAM) << "Async read succeeded - " << running << " running";
                    // send success response only if all the requests have succeeded
                    // note that unsuccessful attempts do not update the counter
                    if (running == 1)
                    {
                        common::Response r(ir, common::ResponseCode::Success);
                        responder->push(std::move(r));
                    }
                }
                else
                {
                    // Note: currently a failure to read any sub range fails the entire read request
                    //       a retry mechanism should be added for failed reads
                    bool previous = is_success->exchange(false);
                    // send error response only once
                    if (previous)
                    {
                        const auto & err = outcome.GetError();
                        LOG(ERROR) << "Failed to download s3 object " << err.GetExceptionName() << ": " << err.GetMessage();
                        common::Response r(ir, common::ResponseCode::FileAccessError);
                        responder->push(std::move(r));
                    }
                }
            });

            total_ -= bytesize_;
            offset_ += bytesize_;
            buffer_ += bytesize_;
        }
        ranges_++;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

std::string S3Client::bucket() const
{
    return std::string(_bucket_name.c_str(), _bucket_name.size());
}

void S3Client::path(const std::string & path)
{
    _path = Aws::String(path.c_str(), path.size());
}

void S3Client::stop()
{
    _stop = true;
    if (_responder != nullptr)
    {
        _responder->stop();
    }
}

}; // namespace runai::llm::streamer::impl::s3
