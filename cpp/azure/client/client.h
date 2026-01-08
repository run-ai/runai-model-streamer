#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <optional>

#include "azure/client_configuration/client_configuration.h"
#include "azure/client/async_azure_client/async_azure_client.h"
#include "common/backend_api/response/response.h"

#include "common/backend_api/object_storage/object_storage.h"
#include "common/client_mgr/client_mgr.h"
#include "common/storage_uri/storage_uri.h"
#include "common/shared_queue/shared_queue.h"
#include "common/range/range.h"

#include <azure/storage/blobs.hpp>

namespace runai::llm::streamer::impl::azure
{

struct AzureClient : common::IClient
{
    AzureClient(const common::backend_api::ObjectClientConfig_t& config);
    ~AzureClient();

    // verify that client's credentials have not changed
    bool verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const;

    common::ResponseCode async_read(const char* path, 
                                    common::backend_api::ObjectRange_t range, 
                                    char* destination_buffer, 
                                    common::backend_api::ObjectRequestId_t request_id);

    common::backend_api::Response async_read_response();

    // Stop sending requests to the object store
    // If stopped before all requests for an async_read() call are sent, subsequent request chunks will not be sent.
    void stop();

 private:
    std::atomic<bool> _stop;
    ClientConfiguration _client_config;
    const size_t _chunk_bytesize;
    
    // Azure credentials (uses DefaultAzureCredential)
    std::optional<std::string> _account_name;
    std::optional<std::string> _endpoint;
    
    // Azure Blob Storage client and async wrapper
    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> _blob_service_client;
    std::unique_ptr<AsyncAzureClient> _async_client;

    // queue of asynchronous responses
    using Responder = common::SharedQueue<common::backend_api::Response>;
    std::shared_ptr<Responder> _responder;
    std::mutex _responder_mutex;  // Protects _responder access
};

} // namespace runai::llm::streamer::impl::azure
