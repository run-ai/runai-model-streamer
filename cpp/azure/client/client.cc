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

// Implementation struct that holds the Azure SDK client
struct AzureClientImpl
{
    std::shared_ptr<BlobServiceClient> blob_service_client;
    std::vector<std::future<void>> active_futures;
    std::mutex futures_mutex;
};

AzureClient::AzureClient(const common::backend_api::ObjectClientConfig_t& config) :
    _stop(false),
    _impl(std::make_unique<AzureClientImpl>()),
    _responder(nullptr),
    _chunk_bytesize(config.default_storage_chunk_size)
{
    // ClientConfiguration reads environment variables
    // Override with explicit parameters if provided
    _connection_string = _client_config.connection_string;
    _account_name = _client_config.account_name;
    _account_key = _client_config.account_key;
    _sas_token = _client_config.sas_token;
    _endpoint = _client_config.endpoint_url;

    // Parse configuration parameters from API (overrides environment)
    auto ptr = config.initial_params;
    if (ptr)
    {
        for (size_t i = 0; i < config.num_initial_params; ++i, ++ptr)
        {
            const char* key = ptr->key;
            const char* value = ptr->value;
            
            if (strcmp(key, "connection_string") == 0)
            {
                _connection_string = std::string(value);
            }
            else if (strcmp(key, "account_name") == 0)
            {
                _account_name = std::string(value);
            }
            else if (strcmp(key, "account_key") == 0)
            {
                _account_key = std::string(value);
            }
            else if (strcmp(key, "sas_token") == 0)
            {
                _sas_token = std::string(value);
            }
            else if (strcmp(key, "endpoint") == 0)
            {
                _endpoint = std::string(value);
            }
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

    // Validate credentials
    bool has_credentials = _connection_string.has_value() || 
                          (_account_name.has_value() && (_account_key.has_value() || _sas_token.has_value()));
    
    if (!has_credentials)
    {
        LOG(WARNING) << "No Azure credentials provided, attempting to use default Azure credential chain";
    }

    // Initialize Azure Blob Storage client with options for Azurite compatibility
    try {
        BlobClientOptions options;
        // Use API version 2023-11-03 which is supported by both Azure and Azurite
        options.ApiVersion = "2023-11-03";

        LOG(INFO) << "Setting Azure SDK API version to: " << options.ApiVersion;

        // Apply retry configuration from ClientConfiguration
        // Reference: https://learn.microsoft.com/en-us/azure/storage/common/storage-retry-policy
        Azure::Core::Http::Policies::RetryOptions retry_options;
        retry_options.MaxRetries = static_cast<int32_t>(_client_config.max_retries);
        retry_options.RetryDelay = std::chrono::milliseconds(_client_config.retry_delay_ms);
        options.Retry = retry_options;
        
        LOG(DEBUG) << "Azure retry policy: max_retries=" << _client_config.max_retries 
                   << ", retry_delay_ms=" << _client_config.retry_delay_ms
                   << ", concurrency=" << _client_config.max_concurrency;

        if (_connection_string.has_value()) {
            _impl->blob_service_client = std::make_shared<BlobServiceClient>(
                BlobServiceClient::CreateFromConnectionString(_connection_string.value(), options)
            );
            LOG(DEBUG) << "Azure client initialized with connection string";
        } else if (_account_name.has_value() && _account_key.has_value()) {
            auto credential = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
                _account_name.value(), _account_key.value()
            );
            std::string url = _endpoint.value_or("https://" + _account_name.value() + ".blob.core.windows.net");
            _impl->blob_service_client = std::make_shared<BlobServiceClient>(url, credential, options);
            LOG(DEBUG) << "Azure client initialized with account key for " << url;
        } else if (_account_name.has_value() && _sas_token.has_value()) {
            std::string url = _endpoint.value_or("https://" + _account_name.value() + ".blob.core.windows.net");
            url += "?" + _sas_token.value();
            _impl->blob_service_client = std::make_shared<BlobServiceClient>(url, options);
            LOG(DEBUG) << "Azure client initialized with SAS token";
        } else {
            // Use default Azure credential (managed identity, Azure CLI, etc.)
            auto credential = std::make_shared<Azure::Identity::DefaultAzureCredential>();
            std::string url = _endpoint.value_or("https://" + _account_name.value_or("") + ".blob.core.windows.net");
            _impl->blob_service_client = std::make_shared<BlobServiceClient>(url, credential, options);
            LOG(DEBUG) << "Azure client initialized with default credential";
        }
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to initialize Azure client: " << e.what();
        throw common::Exception(common::ResponseCode::InvalidParameterError);
    }
}

AzureClient::~AzureClient()
{
    stop();

    // Wait for all async operations to complete
    // Futures hold shared_ptr to blob_service_client, so it stays alive until all tasks complete
    std::vector<std::future<void>> futures_to_wait;
    {
        std::lock_guard<std::mutex> lock(_impl->futures_mutex);
        futures_to_wait = std::move(_impl->active_futures);
    }

    for (auto& future : futures_to_wait) {
        if (future.valid()) {
            future.wait();
        }
    }
}

bool AzureClient::verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const
{
    // TODO: Implement credential verification
    // Compare stored credentials with new config
    AzureClient temp_client(config);
    
    return (_connection_string == temp_client._connection_string &&
            _account_name == temp_client._account_name &&
            _account_key == temp_client._account_key &&
            _sas_token == temp_client._sas_token &&
            _endpoint == temp_client._endpoint);
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

    // Parse Azure URI (azure://container/blob or https://account.blob.core.windows.net/container/blob)
    const auto uri = common::s3::StorageUri(path);
    std::string container_name(uri.bucket);
    std::string blob_name(uri.path);

    size_t total_ = range.length;
    size_t offset_ = range.offset;

    for (unsigned i = 0; i < num_chunks && !_stop; ++i)
    {
        size_t bytesize_ = (i == num_chunks - 1 ? total_ : _chunk_bytesize);

        // Launch async task using std::async (Azure SDK clients are thread-safe)
        // Note: future is moved into a detached state after creation to avoid destructor issues
        auto future = std::async(std::launch::async, [
            blob_service_client = _impl->blob_service_client,  // Capture shared_ptr to keep it alive
            container_name,
            blob_name,
            buffer_,
            bytesize_,
            offset_,
            request_id,
            counter,
            is_success,
            responder,
            chunk_index = i,
            timeout_s = _client_config.request_timeout_s  // Capture timeout from config
        ]() {
            try {
                auto container_client = blob_service_client->GetBlobContainerClient(container_name);
                auto blob_client = container_client.GetBlockBlobClient(blob_name);
                DownloadBlobToOptions download_options;
                download_options.Range = Azure::Core::Http::HttpRange();
                download_options.Range.Value().Offset = offset_;
                download_options.Range.Value().Length = bytesize_;

                // Apply timeout from ClientConfiguration
                // Reference: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-performance-checklist
                Azure::Core::Context context;
                if (timeout_s > 0) {
                    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout_s);
                    context = context.WithDeadline(deadline);
                }
                
                auto response = blob_client.DownloadTo(
                    reinterpret_cast<uint8_t*>(buffer_), bytesize_, download_options, context
                );
                
                if (response.Value.ContentRange.Length.HasValue() && 
                    response.Value.ContentRange.Length.Value() == bytesize_) {
                    const auto running = counter->fetch_sub(1);
                    LOG(SPAM) << "Async read request " << request_id << " succeeded - " << running << " running";
                    
                    if (running == 1) {
                        common::backend_api::Response r(request_id, common::ResponseCode::Success);
                        responder->push(std::move(r));
                    }
                } else {
                    LOG(ERROR) << "Azure blob read size mismatch for request " << request_id;
                    bool previous = is_success->exchange(false);
                    if (previous) {
                        common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                        responder->push(std::move(r));
                    }
                }
            } catch (const Azure::Core::RequestFailedException& e) {
                LOG(ERROR) << "Failed to download Azure blob of request " << request_id << " "
                           << static_cast<int>(e.StatusCode) << ": " << e.what();
                bool previous = is_success->exchange(false);
                if (previous) {
                    common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                    responder->push(std::move(r));
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Failed to read Azure blob of request " << request_id << ": " << e.what();
                bool previous = is_success->exchange(false);
                if (previous) {
                    common::backend_api::Response r(request_id, common::ResponseCode::FileAccessError);
                    responder->push(std::move(r));
                }
            }
        });

        // Store the future to prevent blocking on destruction
        // (std::async futures block in destructor if not stored)
        {
            std::lock_guard<std::mutex> lock(_impl->futures_mutex);
            _impl->active_futures.push_back(std::move(future));
        }
        
        total_ -= bytesize_;
        offset_ += bytesize_;
        buffer_ += bytesize_;
    }

    return _stop ? common::ResponseCode::FinishedError : common::ResponseCode::Success;
}

} // namespace runai::llm::streamer::impl::azure
