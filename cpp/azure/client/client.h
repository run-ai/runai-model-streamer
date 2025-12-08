#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <optional>

#include "azure/client_configuration/client_configuration.h"
#include "common/backend_api/response/response.h"

#include "common/backend_api/object_storage/object_storage.h"
#include "common/client_mgr/client_mgr.h"
#include "common/storage_uri/storage_uri.h"
#include "common/shared_queue/shared_queue.h"
#include "common/range/range.h"

namespace runai::llm::streamer::impl::azure
{

// Forward declaration for pimpl idiom
struct AzureClientImpl;

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
    
    // Azure credentials
    std::optional<std::string> _connection_string;
    std::optional<std::string> _account_name;
    std::optional<std::string> _account_key;
    std::optional<std::string> _sas_token;
    std::optional<std::string> _endpoint;
    
    // Pimpl pointer for Azure SDK client (hides implementation details)
    std::unique_ptr<AzureClientImpl> _impl;

    // queue of asynchronous responses
    using Responder = common::SharedQueue<common::backend_api::Response>;
    std::shared_ptr<Responder> _responder;
    std::mutex _responder_mutex;  // Protects _responder access
};

} // namespace runai::llm::streamer::impl::azure
