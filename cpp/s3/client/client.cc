
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/config/ConfigAndCredentialsCacheManager.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3-crt/model/GetObjectRequest.h>
#include <aws/s3-crt/model/ListObjectsV2Request.h>

#include <cstring>
#include <algorithm>
#include <string>
#include <utility>
#include <optional>

#include "common/backend_api/object_storage/object_storage.h"
#include "s3/client/client.h"

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"
#include "utils/fd/fd.h"

namespace runai::llm::streamer::impl::s3
{

static bool starts_with_ci(const Aws::String & str, const Aws::String & prefix)
{
    if (str.size() < prefix.size()) return false;
    for (size_t i = 0; i < prefix.size(); ++i)
    {
        if (std::tolower(static_cast<unsigned char>(str[i])) !=
            std::tolower(static_cast<unsigned char>(prefix[i])))
            return false;
    }
    return true;
}

EndpointParseResult parse_endpoint_scheme(const Aws::String & endpoint)
{
    const Aws::String https_prefix("https://");
    const Aws::String http_prefix("http://");

    if (starts_with_ci(endpoint, https_prefix))
    {
        return { endpoint.substr(https_prefix.size()), Aws::Http::Scheme::HTTPS };
    }
    if (starts_with_ci(endpoint, http_prefix))
    {
        return { endpoint.substr(http_prefix.size()), Aws::Http::Scheme::HTTP };
    }
    return { endpoint, std::nullopt };
}

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
    _endpoint(convert(config.endpoint_url)),
    _chunk_bytesize(config.default_storage_chunk_size)
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

    // Build explicit credentials when an access key was provided; otherwise leave
    // _client_credentials null so the client falls back to the AWS default
    // credential provider chain (environment, profile, SSO, IMDS, etc.)
    if (_key.has_value() && _secret.has_value())
    {
        _client_credentials = std::make_unique<Aws::Auth::AWSCredentials>(
            _key.value(),
            _secret.value(),
            _token.has_value() ? _token.value() : Aws::String(""));
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
        // Strip the scheme from the endpoint and set it separately.
        // Including http:// in endpointOverride causes the CRT to produce
        // absolute-form request lines (GET http://host/path) which most
        // S3-compatible servers reject.
        auto parsed = parse_endpoint_scheme(_endpoint.value());
        if (parsed.host.empty() || parsed.host[0] == ':')
        {
            LOG(ERROR) << "Invalid endpoint (no host): " << _endpoint.value();
            throw common::Exception(common::ResponseCode::FileAccessError);
        }
        _client_config.config.endpointOverride = parsed.host;
        if (parsed.scheme.has_value())
        {
            _client_config.config.scheme = parsed.scheme.value();
            LOG(DEBUG) << "Setting endpoint override to " << parsed.host
                       << " with scheme " << (parsed.scheme.value() == Aws::Http::Scheme::HTTPS ? "HTTPS" : "HTTP");
        }
        else
        {
            LOG(DEBUG) << "Setting endpoint override to " << parsed.host << " (no explicit scheme)";
        }
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

    // Resolve the TLS CA bundle: the AWS_CA_BUNDLE environment variable takes
    // precedence; otherwise fall back to the "ca_bundle" setting in the shared
    // AWS config profile (~/.aws/config), matching boto3/aws-cli behavior. The
    // config file is already parsed and cached by Aws::InitAPI (see s3_init).
    if (!utils::try_getenv("AWS_CA_BUNDLE", _client_config.config.caFile))
    {
        const auto ca_bundle = Aws::Config::GetCachedConfigValue(Aws::Auth::GetConfigProfileName(), "ca_bundle");
        if (!ca_bundle.empty())
        {
            _client_config.config.caFile = ca_bundle;
        }
    }

    if (!_client_config.config.caFile.empty())
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


common::backend_api::ResponseCode_t S3Client::async_read(const char* path,
                                                         common::backend_api::ObjectRange_t range,
                                                         char* destination_buffer,
                                                         common::backend_api::ObjectRequestId_t request_id)
{
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    const auto uri = common::s3::StorageUri(path);

    Aws::String bucket_name(uri.bucket);
    Aws::String path_name(uri.path);

    char * buffer_ = destination_buffer;
    // split range into chunks
    size_t size = std::max(1UL, range.length/_chunk_bytesize);
    LOG(SPAM) <<"Number of chunks is " << size;

    // each range is divided into chunks (size is the number of chunks)
    // when all the chunks have been read successfuly the response for that range is pushed to the responder

    auto counter = std::make_shared< std::atomic<unsigned> >(size);
    // success flag for the current range is passed to the client
    auto is_success = std::make_shared< std::atomic<bool> >(true);

    size_t total_ = range.length;
    size_t offset_ = range.offset;
    for (unsigned i = 0; i < size && !_stop; ++i)
    {
        size_t bytesize_ = (i == size - 1 ? total_ : _chunk_bytesize);

        // send async request
        auto request = std::make_shared<Aws::S3Crt::Model::GetObjectRequest>();

        request->SetBucket(bucket_name);
        request->SetKey(path_name);
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

common::ResponseCode S3Client::list_files(
    const char* prefix,
    int is_recursive,
    std::vector<std::pair<std::string, size_t>>& results)
{
    const auto uri = common::s3::StorageUri(prefix);

    Aws::S3Crt::Model::ListObjectsV2Request request;
    request.SetBucket(uri.bucket.c_str());
    request.SetPrefix(uri.path.c_str());
    if (!is_recursive)
    {
        request.SetDelimiter("/");
    }

    const std::string uri_prefix = uri.scheme + "://" + uri.bucket + "/";

    bool done = false;
    while (!done)
    {
        auto outcome = _client->ListObjectsV2(request);
        if (!outcome.IsSuccess())
        {
            const auto& err = outcome.GetError();
            LOG(ERROR) << "S3 ListObjectsV2 failed: " << err.GetExceptionName() << ": " << err.GetMessage();
            return common::ResponseCode::FileAccessError;
        }

        const auto& result = outcome.GetResult();
        for (const auto& obj : result.GetContents())
        {
            const auto& key = obj.GetKey();
            // skip zero-byte directory marker objects (keys ending with "/")
            if (!key.empty() && key.back() == '/')
            {
                continue;
            }
            results.emplace_back(uri_prefix + std::string(key), static_cast<size_t>(obj.GetSize()));
        }

        if (result.GetIsTruncated())
        {
            request.SetContinuationToken(result.GetNextContinuationToken());
        }
        else
        {
            done = true;
        }
    }

    return common::ResponseCode::Success;
}

}; // namespace runai::llm::streamer::impl::s3
