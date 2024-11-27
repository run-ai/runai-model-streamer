#include "s3/s3_mock/s3_mock.h"

#include <unistd.h>

#include <map>
#include <mutex>
#include <set>
#include <cstring>
#include <string>
#include <atomic>
#include <vector>

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
std::atomic<bool> __stopped(false);
std::vector<char> __object_data;

void runai_mock_s3_set_response_time_ms(unsigned milliseconds)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);
    __mock_response_time_ms = milliseconds;
}

void runai_mock_s3_set_object_data(const char * data, size_t bytesize)
{
    const auto guard = std::unique_lock<std::mutex>(__mutex);
    __object_data = std::vector<char>(bytesize);
    if (bytesize)
    {
        std::memcpy(__object_data.data(), data, bytesize);
    }
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

    if (__stopped)
    {
        LOG(DEBUG) <<"Mock s3 is stopped";
        return common::ResponseCode::FinishedError;
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
        *index = 0;
        return common::ResponseCode::FinishedError;
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

void runai_stop_s3_clients()
{
    __stopped = true;
    LOG(DEBUG) << "Stopped S3 clients ";
}

void runai_mock_s3_cleanup()
{
    runai_mock_s3_set_response_time_ms(0);
    __stopped = false;
    runai_mock_s3_set_object_data(nullptr, 0);
}

common::ResponseCode runai_list_objects_s3_client(void * client, char*** object_keys, size_t * object_count)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to list objects with null s3 client";
            return common::ResponseCode::UnknownError;
        }

        auto r = get_response_code(client);

        if (r != common::ResponseCode::Success)
        {
            return r;
        }

        auto size = utils::random::number<size_t>(0, 10);
        LOG(SPAM) <<"Listing " << size << " objects";
        std::vector<std::string> strings;
        for (size_t i = 0; i < size; ++i)
        {
            strings.push_back(utils::random::string());
            LOG(SPAM) << i << " object key " << strings[i];
        }

        // Allocate memory for the pointers to the strings
        if (strings.size())
        {
            // malloc and free are used here for ctypes (python integration layer)
            *object_keys = reinterpret_cast<char**>(malloc(strings.size() * sizeof(char*)));
            for (size_t i = 0; i < strings.size(); ++i)
            {
                auto length = strings[i].size() + 1;
                (*object_keys)[i] = reinterpret_cast<char*>(malloc(length)); // Allocate memory for each string
                std::strncpy((*object_keys)[i], strings[i].c_str(), length);
            }
        }
        *object_count = strings.size();
        return r;
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while requesting list of objects";
    }

    return common::ResponseCode::UnknownError;
}

common::ResponseCode runai_object_bytesize_s3_client(void * client, size_t * object_bytesize)
{
    try
    {
        if (!client)
        {
            LOG(ERROR) << "Attempt to list objects with null s3 client";
            return common::ResponseCode::UnknownError;
        }

        auto r = get_response_code(client);

        if (r != common::ResponseCode::Success)
        {
            LOG(ERROR) << "Returning mock error " << r;
            return r;
        }

        const auto guard = std::unique_lock<std::mutex>(__mutex);

        *object_bytesize = (__object_data.size() ? __object_data.size() : utils::random::number<size_t>(1, 100000000));
        LOG(SPAM) << "mock object bytesize " << *object_bytesize;

        return common::ResponseCode::Success;
    }
    catch(const std::exception& e)
    {
        LOG(ERROR) << "Caught exception while requesting size of object";
    }

    return common::ResponseCode::UnknownError;
}

common::ResponseCode runai_read_s3_client(void * client, size_t offset, size_t bytesize, char * buffer)
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
    if (r == common::ResponseCode::Success)
    {
        // read from __object_file_path

        try
        {
            ASSERT(bytesize <= __object_data.size() - offset) << "Attempt to read out of range";
            std::memcpy(buffer, __object_data.data() + offset, bytesize);
        }
        catch(...)
        {
            r = common::ResponseCode::FileAccessError;
        }
    }

    return r;
}

}; //namespace runai::llm::streamer::common::s3
