#include "s3/s3_mock/s3_mock.h"

#include <unistd.h>

#include <map>
#include <mutex>
#include <set>
#include <atomic>

#include "common/s3_credentials/s3_credentials.h"

#include "utils/logging/logging.h"
#include "utils/random/random.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::common::s3
{

std::set<common::backend_api::ObjectClientHandle_t> __mock_clients;
std::map<common::backend_api::ObjectClientHandle_t /* client */, std::set<common::backend_api::ObjectRequestId_t /* request id */>> __mock_client_requests;
std::set<common::backend_api::ObjectClientHandle_t> __mock_unused;
unsigned __mock_response_time_ms = 0;
std::mutex __mutex;
std::atomic<bool> __stopped(false);
std::atomic<bool> __opened(false);

common::backend_api::ResponseCode_t obj_open_backend(common::backend_api::ObjectBackendHandle_t* out_backend_handle)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    auto response_code = common::response_code_from(utils::getenv<int>("RUNAI_STREAMER_S3_MOCK_RESPONSE_CODE", static_cast<int>(common::ResponseCode::Success)));
    if (response_code != common::ResponseCode::Success)
    {
        LOG(ERROR) << "S3 mock backend not opened";
        return response_code;
    }

    if (__opened)
    {
        LOG(ERROR) << "S3 mock backend already opened";
        return common::ResponseCode::UnknownError;
    }

    __opened = true;
    return response_code;
}

common::backend_api::ResponseCode_t obj_close_backend(common::backend_api::ObjectBackendHandle_t backend_handle)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    auto response_code = common::response_code_from(utils::getenv<int>("RUNAI_STREAMER_S3_MOCK_RESPONSE_CODE", static_cast<int>(common::ResponseCode::Success)));
    if (response_code != common::ResponseCode::Success)
    {
        LOG(ERROR) << "S3 mock backend not closed";
        return response_code;
    }

    if (!__opened)
    {
        LOG(ERROR) << "S3 mock backend not opened";
        return common::ResponseCode::UnknownError;
    }

    __opened = false;
    return response_code;
}

void runai_mock_s3_set_response_time_ms(unsigned milliseconds)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);
    __mock_response_time_ms = milliseconds;
}

common::backend_api::ResponseCode_t obj_create_client(
    common::backend_api::ObjectBackendHandle_t backend_handle,
    const common::backend_api::ObjectClientConfig_t* client_initial_config,
    common::backend_api::ObjectClientHandle_t* out_client_handle)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    do
    {
        *out_client_handle = reinterpret_cast<common::backend_api::ObjectClientHandle_t>(utils::random::number());
    } while (__mock_clients.count(*out_client_handle) || __mock_unused.count(*out_client_handle));

    __mock_clients.insert(*out_client_handle);

    if (__mock_client_requests.find(*out_client_handle) != __mock_client_requests.end())
    {
        LOG(ERROR) << "Client " << *out_client_handle << " already exists";
        return common::ResponseCode::UnknownError;
    }

    __mock_client_requests[*out_client_handle] = {};

    LOG(DEBUG) << "created client " << *out_client_handle << " - mock size is " << __mock_client_requests.size();
    return common::ResponseCode::Success;
}

common::backend_api::ResponseCode_t obj_remove_client(common::backend_api::ObjectClientHandle_t client_handle)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    try
    {
        ASSERT(client_handle) << "No client";
        ASSERT(__mock_client_requests.find(client_handle) != __mock_client_requests.end()) << "Client " << client_handle << " not found";
        __mock_client_requests.erase(client_handle);
        __mock_unused.insert(client_handle);
        LOG(DEBUG) << "Removed S3 client " << client_handle << " - mock size is " << __mock_client_requests.size();
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Failed to remove object storage client " << client_handle;
        return common::ResponseCode::UnknownError;
    }
    return common::ResponseCode::Success;
}

common::ResponseCode get_response_code(void * client)
{
    try
    {
        auto response_code = common::response_code_from(utils::getenv<int>("RUNAI_STREAMER_S3_MOCK_RESPONSE_CODE", static_cast<int>(common::ResponseCode::Success)));
        return response_code;
    }
    catch(const std::exception& e)
    {
    }
    return common::ResponseCode::UnknownError;
}

common::ResponseCode  runai_async_read_s3_client(void * client, common::backend_api::ObjectRequestId_t request_id, const common::s3::StorageUri_C * path, common::Range * range, size_t chunk_bytesize, char * buffer)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    if (!__mock_clients.count(client) || __mock_unused.count(client))
    {
        LOG(ERROR) << "Mock client " << client << " not found or unused";
        return common::ResponseCode::UnknownError;
    }

    if (__stopped)
    {
        LOG(DEBUG) <<"Mock s3 is stopped";
        return common::ResponseCode::FinishedError;
    }

    auto r = get_response_code(client);

    ASSERT(__mock_client_requests.find(client) != __mock_client_requests.end()) << "Client " << client << " not found";
    __mock_client_requests[client].insert(request_id);

    return r;
}

common::ResponseCode runai_async_response_s3_client(void * client, common::backend_api::ObjectCompletionEvent_t * event_buffer, unsigned max_events_to_retrieve, unsigned * out_num_events_retrieved)
{
    if (out_num_events_retrieved == nullptr)
    {
        LOG(ERROR) << "output parameter out_num_events_retrieved is null";
        return common::ResponseCode::UnknownError;
    }

    if (event_buffer == nullptr)
    {
        LOG(ERROR) << "output parameter event_buffer is null";
        return common::ResponseCode::UnknownError;
    }

    const auto guard = std::unique_lock<std::mutex>(__mutex);

    if (!__mock_clients.count(client) || __mock_unused.count(client))
    {
        LOG(ERROR) << "Mock client " << client << " not found or unused";
        return common::ResponseCode::UnknownError;
    }

    if (__mock_response_time_ms)
    {
        unsigned counter = 100;
        while (!__stopped && counter > 0)
        {
            LOG(DEBUG) << "Sleeping for " << __mock_response_time_ms << " milliseconds";
            ::usleep(10 * __mock_response_time_ms);
            --counter;
        }
    }

    if (__stopped)
    {
        return common::ResponseCode::FinishedError;
    }

    auto r = get_response_code(client);

    if (__mock_client_requests.find(client) == __mock_client_requests.end())
    {
        LOG(ERROR) << "Mock client " << client << " not found";
        return common::ResponseCode::UnknownError;
    }

    auto & client_requests = __mock_client_requests[client];

    *out_num_events_retrieved = 0;
    for (auto it = client_requests.begin(); it != client_requests.end() && *out_num_events_retrieved < max_events_to_retrieve; )
    {
        event_buffer[*out_num_events_retrieved].request_id = *it;
        event_buffer[*out_num_events_retrieved].response_code = r;
        it = client_requests.erase(it);
        ++*out_num_events_retrieved;
    }
    if (*out_num_events_retrieved == 0)
    {
        LOG(DEBUG) << "No more ranges to return";
        r = common::ResponseCode::FinishedError;
    }

    return r;
}

int runai_mock_s3_clients()
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    return __mock_clients.size();
}

void runai_release_s3_clients()
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);
    if (__mock_clients.size() == __mock_unused.size())
    {
        __mock_clients.clear();
        __mock_unused.clear();
        __mock_client_requests.clear();
    }
}

void runai_stop_s3_clients()
{
    __stopped = true;
    LOG(DEBUG) << "Stopped S3 clients ";
}

void runai_mock_s3_cleanup()
{
    runai_mock_s3_set_response_time_ms(0);
    __stopped = false;
}

}; //namespace runai::llm::streamer::common::s3
