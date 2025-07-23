#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <optional>

#include "s3/client_configuration/client_configuration.h"
#include "common/backend_api/response/response.h"

#include "common/backend_api/object_storage/object_storage.h"
#include "common/storage_uri/storage_uri.h"
#include "common/s3_wrapper/s3_wrapper.h"
#include "common/shared_queue/shared_queue.h"
#include "common/range/range.h"

namespace runai::llm::streamer::impl::s3
{

struct S3ClientBase
{
    S3ClientBase(const common::backend_api::ObjectClientConfig_t & config);

     // verify that clien's credentials have not change
    bool verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const;

 protected:
    std::optional<Aws::String> _key;
    std::optional<Aws::String> _secret;
    std::optional<Aws::String> _token;
    std::optional<Aws::String> _region;
    const std::optional<Aws::String> _endpoint;
    std::unique_ptr<Aws::Auth::AWSCredentials> _client_credentials;
    const size_t _chunk_bytesize;

 private:
    bool verify_credentials_member(const std::optional<Aws::String>& client_member, const std::optional<Aws::String>& input_member, const char * name) const;
};

struct S3Client : S3ClientBase
{
    S3Client(const common::backend_api::ObjectClientConfig_t & config);

    common::backend_api::ResponseCode_t async_read(const char* path,
                                                   common::backend_api::ObjectRange_t range,
                                                   char* destination_buffer,
                                                   common::backend_api::ObjectRequestId_t request_id);

    common::backend_api::Response async_read_response();

    // Stop sending requests to the object store
    // Requests that were already sent cannot be cancelled, since the Aws S3CrtClient does not support aborting requests
    // The S3CrtClient d'tor will wait for response of all teh sent requests, which can take a while
    void stop();

    using S3ClientBase::verify_credentials;

 private:
    std::atomic<bool> _stop;
    ClientConfiguration _client_config;
    std::unique_ptr<Aws::S3Crt::S3CrtClient> _client;

    // queue of asynchronous responses
    using Responder = common::SharedQueue<common::backend_api::Response>;
    std::shared_ptr<Responder> _responder;
};

}; //namespace runai::llm::streamer::impl::s3
