#include <algorithm>
#include <string>
#include <utility>
#include <optional>
#include <vector>
#include <future>
#include <memory>
#include <functional>

#include "google/cloud/storage/client.h"
#include "google/cloud/storage/oauth2/credentials.h"
#include "google/cloud/common_options.h"
#include "google/cloud/status_or.h"

#include "gcs/client/client.h"

#include "common/exception/exception.h"

#include "utils/logging/logging.h"
#include "utils/env/env.h"
#include "utils/fd/fd.h"

namespace runai::llm::streamer::impl::gcs
{

GCSClient::GCSClient(const common::backend_api::ObjectClientConfig_t& config) :
    _stop(false),
    _responder(nullptr),
    _chunk_bytesize(config.default_storage_chunk_size)
{
    _client = std::make_unique<google::cloud::storage::Client>(google::cloud::storage::Client(_client_config.options));
}

bool GCSClient::verify_credentials(const common::backend_api::ObjectClientConfig_t & config) const
{
    // TODO: Verify credentials.
    return true;
}

common::backend_api::Response GCSClient::async_read_response()
{
    if (_responder == nullptr)
    {
        LOG(WARNING) << "Requesting response with uninitialized responder";
        return common::ResponseCode::FinishedError;
    }

    return _responder->pop();
}

common::ResponseCode GCSClient::async_read(const char* path, common::backend_api::ObjectRange_t range, char* destination_buffer, common::backend_api::ObjectRequestId_t request_id)
{
    if (_responder == nullptr)
    {
        _responder = std::make_shared<Responder>(1);
    }
    else
    {
        _responder->increment(1);
    }

    char * buffer_ = destination_buffer;
    // split range into chunks
    size_t size = std::max(1UL, range.length/_chunk_bytesize);
    LOG(SPAM) << "Number of chunks is: " << size;

    // each range is divided into chunks (size is the number of chunks)
    // when all the chunks have been read successfuly the response for that range is pushed to the responder

    const auto uri = common::s3::StorageUri(path);

    std::string bucket_name(uri.bucket);
    std::string path_name(uri.path);

    size_t total_ = range.length;
    size_t offset_ = range.offset;

    auto response_code = common::ResponseCode::Success;
    for (unsigned i = 0; i < size && !_stop; ++i)
    {
        size_t bytesize_ = (i == size - 1 ? total_ : _chunk_bytesize);

        auto stream = _client->ReadObject(bucket_name, path_name, google::cloud::storage::ReadRange(offset_, offset_ + bytesize_));
        size_t bytes_received = 0;
        while (stream.read(buffer_, bytesize_)) {
            bytes_received += stream.gcount();
        }
        stream.Close();
        if (bytes_received != bytesize_) {
            LOG(ERROR) << "GCS ReadObject received " << bytes_received << " bytes, but "
                    << bytesize_ << " were requested. This is unexpected." << std::endl;
            response_code = common::ResponseCode::FileAccessError;
            break;
        }
        if (stream.bad()) {
            // Note: currently a failure to read any sub range fails the entire read request
            //       a retry mechanism should be added for failed reads
            const auto & err = stream.status();
            LOG(ERROR) << "Failed to download GCS object of request " << request_id << " " << err.code() << ": " << err.message();
            response_code = common::ResponseCode::FileAccessError;
            break;
        } else {
            LOG(SPAM) << "Read request [" << request_id << ":" << i << "] succeeded - (" << (size - 1 - i) << "/" << size << ") remaining";
        }

        total_ -= bytesize_;
        offset_ += bytesize_;
        buffer_ += bytesize_;
    }

    common::backend_api::Response r(request_id, response_code);
    _responder->push(std::move(r));

    return _stop ? common::ResponseCode::FinishedError : response_code;
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
