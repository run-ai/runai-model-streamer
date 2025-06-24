
#include <aws/s3-crt/model/GetObjectRequest.h>

#include <cstring>
#include <algorithm>
#include <string>
#include <utility>
#include <optional>

#include "s3/client/client.h"

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"
#include "utils/fd/fd.h"

namespace runai::llm::streamer::impl::s3
{

std::optional<Aws::String> convert(const char * input)
{
    std::optional<Aws::String> result = std::nullopt;
    if (input)
    {
        result = Aws::String(input);
    }
    return result;
}

S3ClientBase::S3ClientBase(const common::backend_api::ObjectClientConfig_t & config) :
    _endpoint(convert(config.endpoint_url))
{
    auto ptr = config.initial_params;
    if (ptr)
    {
        for (size_t i = 0; i < config.num_initial_params; ++i, ++ptr)
        {
            const char* key = ptr->key;
            const char* value = ptr->value;
            if (strcmp(key, common::s3::Credentials::ACCESS_KEY_ID_KEY) == 0)
            {
                _key = convert(value);
            }
            else if (strcmp(key, common::s3::Credentials::SECRET_ACCESS_KEY_KEY) == 0)
            {
                _secret = convert(value);
            }
            else if (strcmp(key, common::s3::Credentials::SESSION_TOKEN_KEY) == 0)
            {
                _token = convert(value);
            }
            else if (strcmp(key, common::s3::Credentials::REGION_KEY) == 0)
            {
                _region = convert(value);
            }
            else
            {
                LOG(WARNING) << "Unknown initial parameter: " << key;
            }
        }
    }
}

bool S3ClientBase::verify_credentials_member(const std::optional<Aws::String>& member, const std::optional<Aws::String>& value, const char * name) const
{
    if (member.has_value())
    {
        if (!value.has_value())
        {
            LOG(DEBUG) << "credentials member " << name << " is set, but provided member is nullptr";
            return false;
        }
        if (member.value() != value.value())
        {
            LOG(DEBUG) << "credentials member " << name << " doesn't match the provided value";
            return false;
        }
    }
    else if (value.has_value()) // must be nullptr and not empty string
    {
        LOG(DEBUG) << "credentials member " << name << " is not set, but provided member is not nullptr";
        return false;
    }
    LOG(DEBUG) << "credentials member " << name << " verified";
    return true;
}

bool S3ClientBase::verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const
{
    S3ClientBase other(config);
    return (verify_credentials_member(_key, other._key, "access key") &&
            verify_credentials_member(_secret, other._secret, "secret") &&
            verify_credentials_member(_token, other._token, "session token") &&
            verify_credentials_member(_region, other._region, "region") &&
            verify_credentials_member(_endpoint, other._endpoint, "endpoint"));
}

S3Client::S3Client(const common::backend_api::ObjectClientConfig_t & config) :
    S3ClientBase(config),
    _stop(false),
    _responder(nullptr)
{
    if (_endpoint.has_value()) // endpoint passed as parameter by user application (in credentials)
    {
        _client_config.config.endpointOverride = _endpoint.value();
    }

    if (utils::try_getenv("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING", _client_config.config.useVirtualAddressing))
    {
        LOG(DEBUG) << "Setting s3 configuration useVirtualAddressing to " << _client_config.config.useVirtualAddressing;
    }

    if (_region.has_value())
    {
        LOG(DEBUG) << "Setting s3 region to " << _region.value();
        _client_config.config.region = _region.value();
    }

    if (utils::try_getenv("AWS_CA_BUNDLE", _client_config.config.caFile))
    {
        LOG(DEBUG) << "Setting s3 configuration ca certificate file to " << _client_config.config.caFile;

        // verify file exists
        if (!utils::Fd::exists(_client_config.config.caFile))
        {
            LOG(ERROR) << "CA cert file not found: " << _client_config.config.caFile;
            throw common::Exception(common::ResponseCode::CaFileNotFound);
        }
    }

    if (_client_credentials == nullptr)
    {
        _client = std::make_unique<Aws::S3Crt::S3CrtClient>(_client_config.config);
        LOG(DEBUG) << "Using default authentication";
    }
    else
    {
        LOG(DEBUG) << "Creating S3 client with given credentials";
        _client = std::make_unique<Aws::S3Crt::S3CrtClient>(*_client_credentials, _client_config.config);
    }
}

// returns response object that contains the index of the range in ranges vector  which was passed in the request (0... number of ranges - 1)
common::backend_api::Response S3Client::async_read_response()
{
    if (_responder == nullptr)
    {
        LOG(WARNING) << "Requesting response with uninitialized responder";
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

// aynchronously read consecutive ranges, producing a Response object per range in the ranges vector
common::ResponseCode S3Client::async_read(const common::s3::StorageUri_C * uri, common::backend_api::ObjectRequestId_t request_id, const common::Range & range, size_t chunk_bytesize, char * buffer)
{
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    // TO DO(Noa) - is this needed?
    //ASSERT((!_endpoint.has_value()) || (uri->endpoint == nullptr) || (_endpoint.has_value() && _endpoint.value() == std::string(uri->endpoint))) << "Attempting to reuse client with a different endpoint " << uri->endpoint;

    Aws::String bucket_name(uri->bucket);
    Aws::String path(uri->path);

    char * buffer_ = buffer;
    // split range into chunks
    size_t size = std::max(1UL, range.size/chunk_bytesize);
    LOG(SPAM) <<"Number of chunks is " << size;

    // each range is divided into chunks (size is the number of chunks)
    // when all the chunks have been read successfuly the response for that range is pushed to the responder

    auto counter = std::make_shared< std::atomic<unsigned> >(size);
    // success flag for the current range is passed to the client
    auto is_success = std::make_shared< std::atomic<bool> >(true);

    size_t total_ = range.size;
    size_t offset_ = range.start;
    for (unsigned i = 0; i < size && !_stop; ++i)
    {
        size_t bytesize_ = (i == size - 1 ? total_ : chunk_bytesize);

        // send async request
        auto request = std::make_shared<Aws::S3Crt::Model::GetObjectRequest>();

        request->SetBucket(bucket_name);
        request->SetKey(path);
        std::string range_str = "bytes=" + std::to_string(offset_) + "-" + std::to_string(offset_ + bytesize_ - 1);
        request->SetRange(range_str.c_str());

        request->SetResponseStreamFactory(
            [buffer_, bytesize_]()
            {
                std::unique_ptr<Aws::StringStream>
                        stream(Aws::New<Aws::StringStream>("RunaiBuffer"));

                stream->rdbuf()->pubsetbuf(buffer_, bytesize_);

                return stream.release();
            });

        _client->GetObjectAsync(*request, [request, responder = _responder, request_id, counter, is_success](const Aws::S3Crt::S3CrtClient*, const Aws::S3Crt::Model::GetObjectRequest&,
                                                                        const Aws::S3Crt::Model::GetObjectOutcome& outcome,
                                                                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>&) {
            if (outcome.IsSuccess())
            {
                const auto running = counter->fetch_sub(1);
                LOG(SPAM) << "Async read request " << request_id << " succeeded - " << running << " running";
                // send success response only if all the requests have succeeded
                // note that unsuccessful attempts do not update the counter
                if (running == 1)
                {
                    common::backend_api::Response r(request_id, common::ResponseCode::Success);
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
                    LOG(ERROR) << "Failed to download s3 object of request " << request_id << " " << err.GetExceptionName() << ": " << err.GetMessage();
                    common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                    responder->push(std::move(r));
                }
            }
        });

        total_ -= bytesize_;
        offset_ += bytesize_;
        buffer_ += bytesize_;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
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
