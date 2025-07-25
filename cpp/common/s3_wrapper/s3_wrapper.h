#pragma once

#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include "common/range/range.h"
#include "common/response_code/response_code.h"
#include "common/storage_uri/storage_uri.h"
#include "common/backend_api/object_storage/object_storage.h"
#include "common/backend_api/response/response.h"
#include "common/s3_credentials/s3_credentials.h"

#include "utils/dylib/dylib.h"
#include "utils/semver/semver.h"

namespace runai::llm::streamer::common::s3
{

struct S3ClientWrapper
{
      struct Params
      {
         Params()
         {}

         Params(std::shared_ptr<StorageUri> uri, const Credentials & credentials, size_t chunk_bytesize);

         Params(std::shared_ptr<StorageUri> uri, size_t chunk_bytesize) : Params(uri, Credentials(), chunk_bytesize)
         {}

         bool valid() const { return (uri.get() != nullptr); }

         size_t chunk_bytesize;
         std::shared_ptr<StorageUri> uri;
         Credentials credentials;
         const common::backend_api::ObjectClientConfig_t to_config(std::vector<common::backend_api::ObjectConfigParam_t> & initial_params) const;

       private:
         std::string _endpoint;
      };

      struct BackendHandle
      {
         BackendHandle(const Params & params);

         ~BackendHandle();

         common::backend_api::ObjectBackendHandle_t backend_handle() const;

         std::shared_ptr<utils::Dylib> open_object_storage_impl(const Params & params);

         std::shared_ptr<utils::Dylib> dylib_ptr;

       private:
         common::backend_api::ObjectBackendHandle_t _backend_handle;
      };

      S3ClientWrapper(const Params & params);
      ~S3ClientWrapper();

      // request to read a continous range into a buffer
      // the range is divided into sub ranges, which will generate response whenever a full sub range is fully read
      // ranges - list of sub ranges
      // chunk_bytesize - size of chunk for reading in multi parts (minimal size is 5 MB)

      common::ResponseCode async_read(const Params & params, backend_api::ObjectRequestId_t request_id, const Range & ranges, char * buffer);
      common::ResponseCode async_read_response(std::vector<backend_api::ObjectCompletionEvent_t> & event_buffer, unsigned max_events_to_retrieve);

      // stop - stops the responder of each S3 client, in order to notify callers which sent a request and are waiting for a response
      //        required for stopping the threadpool workers, which are bloking on the client responder
      static void stop();

      // destroy S3 all clients
      static void shutdown();

      static constexpr size_t min_chunk_bytesize = 5 * 1024 * 1024;
      static constexpr size_t default_chunk_bytesize = 8 * 1024 * 1024;

 private:
      void * create_client(const Params & params);
      enum class ManageBackendHandleOp
      {
         CREATE,
         DESTROY,
      };
      static std::shared_ptr<BackendHandle> manage_backend_handle(const Params & params, ManageBackendHandleOp op);
      static common::backend_api::ObjectShutdownPolicy_t get_backend_shutdown_policy(std::shared_ptr<BackendHandle> handle);

 private:
      static std::mutex _backend_handle_mutex;
      std::shared_ptr<BackendHandle> _backend_handle;

      // Handle to s3 client
      void * _s3_client;
};

}; //namespace runai::llm::streamer::common::s3
