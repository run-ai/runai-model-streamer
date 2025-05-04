#pragma once

#include <set>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "utils/logging/logging.h"

#include "s3/s3_init/s3_init.h"
#include "s3/client/client.h"

namespace runai::llm::streamer::impl::s3
{

// Singleton clients manager for reusing clients of the current S3 bucket

template <typename T>
struct ClientMgr
{
    ~ClientMgr();

    ClientMgr<T> & operator=(const ClientMgr<T> &) = delete;

    static T* pop(const common::s3::Path & path, const common::s3::Credentials_C & credentials);
    static void push(T* client);

    static void clear();

    // stop all clients
    static void stop();

    // for testing:
    static unsigned size();
    static unsigned unused();
    static std::string current_bucket();

 private:
    ClientMgr();

    static ClientMgr<T> & get();

    // shutdown of S3 will occur when the main process exits
    S3Init _s3;

    mutable std::mutex _mutex;

    // reuse clients of the same bucket
    std::string _current_bucket;
    std::set<T*> _bucket_unused_clients;
    std::map<T*, std::unique_ptr<T>> _clients;
};

template <typename T>
ClientMgr<T>::ClientMgr()
{}

template <typename T>
ClientMgr<T>::~ClientMgr()
{}

template <typename T>
ClientMgr<T> & ClientMgr<T>::get()
{
    static ClientMgr<T> __clients_mgr;
    return __clients_mgr;
}

template <typename T>
unsigned ClientMgr<T>::size()
{
    auto & mgr = get();

    const auto guard = std::unique_lock<std::mutex>(mgr._mutex);
    return mgr._clients.size();
}

template <typename T>
unsigned ClientMgr<T>::unused()
{
    auto & mgr = get();

    const auto guard = std::unique_lock<std::mutex>(mgr._mutex);
    return mgr._bucket_unused_clients.size();
}

template <typename T>
std::string ClientMgr<T>::current_bucket()
{
    auto & mgr = get();

    const auto guard = std::unique_lock<std::mutex>(mgr._mutex);
    return mgr._current_bucket;
}


template <typename T>
T* ClientMgr<T>::pop(const common::s3::Path & path, const common::s3::Credentials_C & credentials)
{
    auto & mgr = get();

    {
        const auto guard = std::unique_lock<std::mutex>(mgr._mutex);

        auto & unused = mgr._bucket_unused_clients;
        bool is_bucket = path.uri.bucket == mgr._current_bucket;
        if (is_bucket)
        {
            while (!unused.empty())
            {
                auto ptr = *unused.begin();
                ptr->path(path);
                unused.erase(unused.begin());

                // Reuse client only if credentials have not changed
                if (ptr->verify_credentials(credentials))
                {
                    LOG(DEBUG) << "Reusing S3 client";
                    return ptr;
                }

                // release the stale client
                mgr._clients.erase(ptr);
            }
        }
        else
        {
            // remove unused clients of other buckets
            for (T* client : unused)
            {
                mgr._clients.erase(client);
            }
            unused.clear();
            mgr._current_bucket = path.uri.bucket;
        }
    }

    // create new client if there are no unused clients for this bucket

    LOG(DEBUG) << "Creating client for path " << path.uri.path << " index " << path.index;
    auto client = std::make_unique<T>(path, credentials);

    const auto guard = std::unique_lock<std::mutex>(mgr._mutex);
    auto ptr = client.get();
    mgr._clients[ptr] = std::move(client);
    return ptr;
}

template <typename T>
void ClientMgr<T>::push(T* client)
{
    LOG(DEBUG) << "Releasing S3 client";
    auto & mgr = get();
    const auto guard = std::unique_lock<std::mutex>(mgr._mutex);
    if (mgr._current_bucket == client->bucket())
    {
        mgr._bucket_unused_clients.insert(client);
    }
    else
    {
        mgr._clients.erase(client);
    }
}

template <typename T>
void ClientMgr<T>::clear()
{
    LOG(DEBUG) << "Releasing all S3 clients";
    auto & mgr = get();

    const auto guard = std::unique_lock<std::mutex>(mgr._mutex);

    // verify that all clients are unused
    unsigned count = mgr._bucket_unused_clients.size();

    if (count != mgr._clients.size())
    {
        LOG(ERROR) << "There are used S3 clients - number of clients is " << mgr._clients.size() << " while number of unused clients is " << count;
        return;
    }

    mgr._clients.clear();
    mgr._bucket_unused_clients.clear();
    mgr._current_bucket.clear();
}

template <typename T>
void ClientMgr<T>::stop()
{
    LOG(DEBUG) << "Stopping all S3 clients";
    auto & mgr = get();

    const auto guard = std::unique_lock<std::mutex>(mgr._mutex);

    for (auto & pair : mgr._clients)
    {
        pair.second->stop();
    }
}

using S3ClientMgr = ClientMgr<S3Client>;

}; //namespace runai::llm::streamer::impl::s3
