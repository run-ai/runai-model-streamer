#include <algorithm>
#include <string>
#include <utility>
#include <optional>
#include <vector>
#include <future>
#include <memory>
#include <functional>

#include "google/cloud/storage/client.h"
#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/async/reader_connection.h"
#include "google/cloud/storage/oauth2/credentials.h"
#include "google/cloud/common_options.h"
#include "google/cloud/grpc_options.h"
#include "google/cloud/future.h"
#include "google/cloud/status_or.h"

#include "gcs/client/client.h"

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"
#include "utils/fd/fd.h"

namespace runai::llm::streamer::impl::gcs
{

GCSClient::GCSClient(const common::s3::Path & path) :
    _stop(false),
    _responder(nullptr)
{
    _client = google::cloud::storage_experimental::AsyncClient(_client_config.options);
}

// returns response object that contains the index of the range in ranges vector  which was passed in the request (0... number of ranges - 1)
common::backend_api::Response GCSClient::async_read_response()
{
    if (_responder == nullptr)
    {
        LOG(WARNING) << "Requesting response with uninitialized responder";
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

// aynchronously read consecutive ranges, producing a Response object per range in the ranges vector
common::ResponseCode GCSClient::async_read(const common::s3::Path & object_path, common::backend_api::ObjectRequestId_t request_id, const common::Range & range, size_t chunk_bytesize, char * buffer)
{
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    //ASSERT((!_endpoint.has_value()) || (object_path.uri.endpoint == nullptr) || (_endpoint.has_value() && _endpoint.value() == std::string(object_path.uri.endpoint))) << "Attempting to reuse client with a different endpoint " << object_path.uri.endpoint;

    char * buffer_ = buffer;
    // split range into chunks
    size_t size = std::max(1UL, range.size/chunk_bytesize);
    LOG(SPAM) <<"Number of chunks is " << size;

    // each range is divided into chunks (size is the number of chunks)
    // when all the chunks have been read successfuly the response for that range is pushed to the responder

    auto counter = std::make_shared< std::atomic<unsigned> >(size);
    // success flag for the current range is passed to the client
    auto is_success = std::make_shared< std::atomic<bool> >(true);

    google::cloud::storage_experimental::BucketName bucket_name(object_path.uri.bucket);
    std::string path(object_path.uri.path);

    size_t total_ = range.size;
    size_t offset_ = range.start;
    for (unsigned i = 0; i < size && !_stop; ++i)
    {
        size_t bytesize_ = (i == size - 1 ? total_ : chunk_bytesize);

        _client.ReadObjectRange(
            bucket_name,
            path,
            offset_,
            bytesize_
        ).then([responder = _responder,
                request_id,
                counter,
                is_success,
                bytesize_,
                buffer_]
            (auto result_future) {
            auto result = result_future.get();
            if (result.ok())
            {
                google::cloud::storage_experimental::ReadPayload payload = result.value();
                const auto running = counter->fetch_sub(1);
                LOG(SPAM) << "Async read request " << request_id << " succeeded - " << running << " running";

                size_t chunk_offset = 0;
                for (auto chunk : payload.contents()) {
                    size_t bytes_received = chunk_offset + chunk.size();
                    if (bytes_received > bytesize_) {
                        LOG(WARNING) << "GCS read received at least " << bytes_received << " bytes, but only "
                                << bytesize_ << " were requested for this chunk. This is unexpected."
                                << " Discarding data!" << std::endl;
                    } else {
                        memcpy(buffer_ + chunk_offset, chunk.data(), chunk.size());
                    }
                    chunk_offset += chunk.size();
                }

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
                    const auto & err = result.status();
                    LOG(ERROR) << "Failed to download GCS object of request " << request_id << " " << err.code() << ": " << err.message();
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

void GCSClient::stop()
{
    _stop = true;
    if (_responder != nullptr)
    {
        _responder->stop();
    }
}

}; // namespace runai::llm::streamer::impl::gcs
