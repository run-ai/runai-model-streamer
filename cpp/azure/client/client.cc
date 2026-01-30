#include <algorithm>
#include <string>
#include <utility>
#include <optional>
#include <vector>
#include <future>
#include <mutex>
#include <memory>
#include <functional>
#include <cstring>

#include <azure/storage/blobs.hpp>
#include <azure/identity/default_azure_credential.hpp>
#include <azure/core/exception.hpp>

#include "azure/client/client.h"
#include "common/exception/exception.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

using namespace Azure::Storage::Blobs;

namespace runai::llm::streamer::impl::azure
{

constexpr char kAzureApplicationId[] = "azpartner-runai";

AzureClient::AzureClient(const common::backend_api::ObjectClientConfig_t& config) :
    _stop(false),
    _responder(nullptr),
    _chunk_bytesize(config.default_storage_chunk_size)
{
    // ClientConfiguration reads environment variables
    _account_name = _client_config.account_name;
    _endpoint = _client_config.endpoint_url;
#ifdef AZURITE_TESTING
    _connection_string = _client_config.connection_string;
#endif

    // Parse configuration parameters from API (overrides environment)
    auto ptr = config.initial_params;
    if (ptr)
    {
        for (size_t i = 0; i < config.num_initial_params; ++i, ++ptr)
        {
            const char* key = ptr->key;
            const char* value = ptr->value;

            if (strcmp(key, "account_name") == 0)
            {
                _account_name = std::string(value);
            }
            else if (strcmp(key, "endpoint") == 0)
            {
                _endpoint = std::string(value);
            }
#ifdef AZURITE_TESTING
            else if (strcmp(key, "connection_string") == 0)
            {
                _connection_string = std::string(value);
            }
#endif
            else
            {
                LOG(WARNING) << "Unknown Azure parameter: " << key;
            }
        }
    }

    // Config endpoint_url overrides everything
    if (config.endpoint_url)
    {
        _endpoint = std::string(config.endpoint_url);
    }

    // Initialize Azure Blob Storage client
    try {
        BlobClientOptions options;

        // Set application ID for telemetry (prepended to User-Agent)
        // Reference: https://azure.github.io/azure-sdk/general_azurecore.html#user-agent-format
        options.Telemetry.ApplicationId = kAzureApplicationId;

        // Using Azure SDK defaults for retry policy
        // Reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-retry-policy

        LOG(DEBUG) << "Azure client concurrency: " << _client_config.max_concurrency;

#ifdef AZURITE_TESTING
        if (_connection_string.has_value()) {
            // Connection string authentication (for Azurite testing only)
            // For Azurite, use --skipApiVersionCheck flag instead of setting API version
            // Reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
            _blob_service_client = std::make_shared<BlobServiceClient>(
                BlobServiceClient::CreateFromConnectionString(_connection_string.value(), options)
            );
            LOG(DEBUG) << "Azure client initialized with connection string (Azurite testing)";
        } else
#endif
        if (_account_name.has_value()) {
            // Use DefaultAzureCredential (managed identity, Azure CLI, environment variables, etc.)
            // Reference: https://learn.microsoft.com/en-us/azure/developer/cpp/sdk/authentication
            std::string url = _endpoint.value_or("https://" + _account_name.value() + ".blob.core.windows.net");
            auto credential = std::make_shared<Azure::Identity::DefaultAzureCredential>();
            _blob_service_client = std::make_shared<BlobServiceClient>(url, credential, options);
            LOG(DEBUG) << "Azure client initialized with DefaultAzureCredential for account: " << _account_name.value();
        } else {
#ifdef AZURITE_TESTING
            LOG(ERROR) << "Azure credentials required. Set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME.";
#else
            LOG(ERROR) << "Azure account name is required. Set AZURE_STORAGE_ACCOUNT_NAME environment variable.";
#endif
            throw common::Exception(common::ResponseCode::InvalidParameterError);
        }

        // Create async client with ThreadPool
        _async_client = std::make_unique<AsyncAzureClient>(_blob_service_client, _client_config.max_concurrency);

    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to initialize Azure client: " << e.what();
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }
}

AzureClient::~AzureClient()
{
    stop();
    // AsyncAzureClient destructor will handle cleanup of ThreadPool
}

bool AzureClient::verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const
{
    // Parse config credentials without creating full client
    ClientConfiguration temp_config;
    std::optional<std::string> temp_account_name = temp_config.account_name;
    std::optional<std::string> temp_endpoint = temp_config.endpoint_url;
#ifdef AZURITE_TESTING
    std::optional<std::string> temp_connection_string = temp_config.connection_string;
#endif

    // Override with config parameters
    auto ptr = config.initial_params;
    if (ptr) {
        for (size_t i = 0; i < config.num_initial_params; ++i, ++ptr) {
            if (strcmp(ptr->key, "account_name") == 0) temp_account_name = std::string(ptr->value);
            else if (strcmp(ptr->key, "endpoint") == 0) temp_endpoint = std::string(ptr->value);
#ifdef AZURITE_TESTING
            else if (strcmp(ptr->key, "connection_string") == 0) temp_connection_string = std::string(ptr->value);
#endif
        }
    }
    if (config.endpoint_url) temp_endpoint = std::string(config.endpoint_url);

#ifdef AZURITE_TESTING
    if (_connection_string.has_value()) {
        return (_connection_string == temp_connection_string);
    }
#endif
    return (_account_name == temp_account_name || _endpoint == temp_endpoint);
}

common::backend_api::Response AzureClient::async_read_response()
{
    std::shared_ptr<Responder> responder;
    {
        std::lock_guard<std::mutex> lock(_responder_mutex);
        if (_responder == nullptr)
        {
            LOG(WARNING) << "Requesting response with uninitialized responder";
            return common::ResponseCode::FinishedError;
        }
        responder = _responder;
    }

    return responder->pop();
}

void AzureClient::stop()
{
    _stop = true;
    std::lock_guard<std::mutex> lock(_responder_mutex);
    if (_responder != nullptr)
    {
        _responder->stop();
    }
}

common::ResponseCode AzureClient::async_read(const char* path, 
                                             common::backend_api::ObjectRange_t range, 
                                             char* destination_buffer, 
                                             common::backend_api::ObjectRequestId_t request_id)
{
    std::shared_ptr<Responder> responder;
    {
        std::lock_guard<std::mutex> lock(_responder_mutex);
        if (_responder == nullptr)
        {
            _responder = std::make_shared<Responder>(1);
        }
        else
        {
            _responder->increment(1);
        }
        responder = _responder;
    }

    char * buffer_ = destination_buffer;

    // Split range into chunks
    size_t num_chunks = std::max(1UL, range.length / _chunk_bytesize);
    LOG(SPAM) << "Number of chunks is: " << num_chunks;

    // Counter for tracking chunk completions
    auto counter = std::make_shared<std::atomic<unsigned>>(num_chunks);
    auto is_success = std::make_shared<std::atomic<bool>>(true);

    // Parse Azure URI az://container/blob
    const auto uri = common::s3::StorageUri(path);
    std::string container_name(uri.bucket);
    std::string blob_name(uri.path);

    size_t total_ = range.length;
    size_t offset_ = range.offset;

    for (unsigned i = 0; i < num_chunks && !_stop; ++i)
    {
        size_t bytesize_ = (i == num_chunks - 1 ? total_ : _chunk_bytesize);

        // Capture current buffer, offset for this specific chunk
        char* chunk_buffer = buffer_;
        size_t chunk_offset = offset_;
        
        // Launch async download with callback - AsyncAzureClient ThreadPool handles both download and callback
        _async_client->DownloadBlobRangeAsync(
            container_name,
            blob_name,
            chunk_buffer,
            chunk_offset,
            bytesize_,
            [request_id, counter, is_success, responder](bool success, const std::string& error_msg) {
                if (success) {
                    const auto running = counter->fetch_sub(1);
                    LOG(SPAM) << "Async read request " << request_id << " succeeded - " << running << " running";
                    
                    if (running == 1) {
                        common::backend_api::Response r(request_id, common::ResponseCode::Success);
                        responder->push(std::move(r));
                    }
                } else {
                    LOG(ERROR) << "Failed to download Azure blob of request " << request_id << ": " << error_msg;
                    bool previous = is_success->exchange(false);
                    if (previous) {
                        common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                        responder->push(std::move(r));
                    }
                }
            }
        );
        
        total_ -= bytesize_;
        offset_ += bytesize_;
        buffer_ += bytesize_;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

} // namespace runai::llm::streamer::impl::azure
