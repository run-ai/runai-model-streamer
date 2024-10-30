#include "s3/s3_mock/s3_mock.h"

#include <unistd.h>

#include <map>
#include <mutex>
#include <set>

#include "utils/random/random.h"
#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::common::s3
{

std::set<void *> __mock_clients;
std::map<void *, unsigned> __mock_index;
std::set<void *> __mock_unused;
unsigned __mock_response_time_ms = 0;
std::mutex __mutex;

void runai_mock_s3_set_response_time_ms(unsigned milliseconds)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);
    __mock_response_time_ms = milliseconds;
}


void * runai_create_s3_client(const common::s3::StorageUri & uri)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    void * client;
    do
    {
        client = reinterpret_cast<void *>(utils::random::number());
    } while (__mock_clients.count(client));

    __mock_clients.insert(client);
    __mock_index[client] = 0;
    return client;
}

void runai_remove_s3_client(void * client)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    try
    {
        __mock_index.erase(client);
        __mock_unused.insert(client);
        LOG(DEBUG) << "Removed S3 client - mock size is " << __mock_clients.size();
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

common::ResponseCode  runai_async_read_s3_client(void * client, unsigned num_ranges, common::Range * ranges, size_t chunk_bytesize, char * buffer)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);

    if (!__mock_clients.count(client) || __mock_unused.count(client))
    {
        LOG(ERROR) << "Mock client " << client << " not found or unused";
        return common::ResponseCode::UnknownError;
    }

    auto r = get_response_code(client);
    if (r == common::ResponseCode::Success)
    {
        __mock_index[client] = num_ranges;
    }

    return r;
}

common::ResponseCode  runai_async_response_s3_client(void * client, unsigned * index)
{
    if (index == nullptr)
    {
        LOG(ERROR) << "output parameter index is null";
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
        LOG(DEBUG) << "Sleeping for " << __mock_response_time_ms << " milliseconds";
        ::usleep(1000 * __mock_response_time_ms);
    }

    auto r = get_response_code(client);

    if (r == common::ResponseCode::Success)
    {
        if (__mock_index[client] == 0)
        {
            r = common::ResponseCode::FinishedError;
        }
        else
        {
            *index = __mock_index.at(client) - 1;
            __mock_index[client]--;
        }
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
        __mock_index.clear();
    }
}

}; //namespace runai::llm::streamer::common::s3
