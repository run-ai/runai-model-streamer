
#include <aws/s3-crt/model/GetObjectRequest.h>

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

S3ClientBase::S3ClientBase(const common::s3::Path & path) : S3ClientBase(path, common::s3::Credentials())
{}

std::optional<Aws::String> convert(const char * input) {
    if (input)
    {
        return Aws::String(input);
    }
    return std::nullopt;
}

S3ClientBase::S3ClientBase(const common::s3::Path & path, const common::s3::Credentials_C & credentials) :
    _key(convert(credentials.access_key_id)),
    _secret(convert(credentials.secret_access_key)),
    _token(convert(credentials.session_token)),
    _region(convert(credentials.region)),
    _endpoint(convert(credentials.endpoint)),
    _client_credentials(_key.has_value() && _secret.has_value() ? std::make_unique<Aws::Auth::AWSCredentials>(_key.value(), _secret.value(), (_token.has_value() ? _token.value() : Aws::String(""))) : nullptr)
{
}

bool S3ClientBase::verify_credentials_member(const std::optional<Aws::String>& member, const char* value, const char * name) const
{
    if (member.has_value())
    {
        if (value == nullptr)
        {
            LOG(DEBUG) << "credentials member " << name << " is set, but provided member is nullptr";
            return false;
        }
        if (member.value() != value)
        {
            LOG(DEBUG) << "credentials member " << name << " doesn't match the provided value";
            return false;
        }
    }
    else if (value != nullptr) // must be nullptr and not empty string
    {
        LOG(DEBUG) << "credentials member " << name << " is not set, but provided member is not nullptr";
        return false;
    }
    LOG(DEBUG) << "credentials member " << name << " verified";
    return true;
}

bool S3ClientBase::verify_credentials(const common::s3::Credentials_C & credentials) const
{
    return (verify_credentials_member(_key, credentials.access_key_id, " access key") &&
            verify_credentials_member(_secret, credentials.secret_access_key, "secret") &&
            verify_credentials_member(_token, credentials.session_token, "session token") &&
            verify_credentials_member(_region, credentials.region, "region") &&
            verify_credentials_member(_endpoint, credentials.endpoint, "endpoint"));
}

S3Client::S3Client(const common::s3::Path & path) : S3Client(path, common::s3::Credentials())
{}

S3Client::S3Client(const common::s3::Path & path, const common::s3::Credentials_C & credentials) :
    S3ClientBase(path, credentials),
    _stop(false),
    _responder(nullptr)
{
    if (_endpoint.has_value()) // endpoint passed as parameter by user application
    {
        LOG(DEBUG) <<"Using credentials endpoint " << credentials.endpoint;
        _client_config.config.endpointOverride = _endpoint.value();
    }
    else if (path.uri.endpoint != nullptr) // endpoint passed as environment variable
    {
        bool override_endpoint_flag = utils::getenv<bool>("RUNAI_STREAMER_OVERRIDE_ENDPOINT_URL", true);
        if (override_endpoint_flag)
        {
            _client_config.config.endpointOverride = Aws::String(path.uri.endpoint);
        }
        LOG(DEBUG) <<"Using environment variable endpoint " << path.uri.endpoint << (override_endpoint_flag ? " , using configuration parameter endpointOverride" : "");
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
common::ResponseCode S3Client::async_read(const common::s3::Path & object_path, common::backend_api::ObjectRequestId_t request_id, const common::Range & range, size_t chunk_bytesize, char * buffer)
{
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    ASSERT((!_endpoint.has_value()) || (object_path.uri.endpoint == nullptr) || (_endpoint.has_value() && _endpoint.value() == std::string(object_path.uri.endpoint))) << "Attempting to reuse client with a different endpoint " << object_path.uri.endpoint;

    Aws::String bucket_name(object_path.uri.bucket);
    Aws::String path(object_path.uri.path);

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
