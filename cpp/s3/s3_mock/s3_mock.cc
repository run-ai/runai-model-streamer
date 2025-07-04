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

std::set<void *> __mock_clients;
std::map<void * /* client */, std::set<common::backend_api::ObjectRequestId_t /* request id */>> __mock_client_requests;
std::set<void *> __mock_unused;
unsigned __mock_response_time_ms = 0;
std::mutex __mutex;
std::atomic<bool> __stopped(false);

void runai_mock_s3_set_response_time_ms(unsigned milliseconds)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);
    __mock_response_time_ms = milliseconds;
}

common::ResponseCode runai_create_s3_client(const common::s3::Path * path, const common::s3::Credentials_C * credentials, void ** client)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    do
    {
        *client = reinterpret_cast<void *>(utils::random::number());
    } while (__mock_clients.count(*client) || __mock_unused.count(*client));

    __mock_clients.insert(*client);

    if (__mock_client_requests.find(*client) != __mock_client_requests.end())
    {
        LOG(ERROR) << "Client " << *client << " already exists";
        return common::ResponseCode::UnknownError;
    }

    __mock_client_requests[*client] = {};

    LOG(DEBUG) << "created client " << *client << " - mock size is " << __mock_client_requests.size();
    return common::ResponseCode::Success;
}

void runai_remove_s3_client(void * client)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    try
    {
        ASSERT(client) << "No client";
        ASSERT(__mock_client_requests.find(client) != __mock_client_requests.end()) << "Client " << client << " not found";
        __mock_client_requests.erase(client);
        __mock_unused.insert(client);
        LOG(DEBUG) << "Removed S3 client " << client << " - mock size is " << __mock_client_requests.size();
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Client not found";
    }
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

common::ResponseCode  runai_async_read_s3_client(void * client, common::backend_api::ObjectRequestId_t request_id, const common::s3::Path * path, common::Range * range, size_t chunk_bytesize, char * buffer)
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
