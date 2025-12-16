#include <functional>
#include <thread>
#include <utility>

#include "gcs/client/async_gcs_client/async_gcs_client.h"

namespace runai::llm::streamer::impl::gcs
{

void ReadObjectTask::execute() {
    auto stream = taskFn();
    promise.set_value(std::move(stream));
}

AsyncGcsClient::AsyncGcsClient(google::cloud::Options opts, unsigned int max_pool_size) :
    _pool([&](ReadObjectTask&& task, std::atomic<bool> & stopped)
        {
            task.execute();
        }, max_pool_size)
{
    _client = std::make_unique<google::cloud::storage::Client>(opts);
}

// Implementation of the private helper function.
void AsyncGcsClient::push_task(ReadObjectTask&& task)
{
    _pool.push(std::move(task));
}

}; // namespace runai::llm::streamer::impl::gcs
