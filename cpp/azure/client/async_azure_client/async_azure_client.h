#pragma once

#include <future>
#include <memory>
#include <string>
#include <functional>

#include <azure/storage/blobs.hpp>

#include "utils/threadpool/threadpool.h"

namespace runai::llm::streamer::impl::azure
{

typedef std::function<void()> DownloadBlobFn;
typedef std::function<void(bool success, const std::string& error_msg)> CompletionCallback;

struct DownloadBlobTask {
    DownloadBlobFn taskFn;
    CompletionCallback callback;

public:
    void execute();
};

static DownloadBlobFn createDownloadBlobFn(
    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> client,
    const std::string& container_name,
    const std::string& blob_name,
    char* buffer,
    size_t offset,
    size_t length)
{
    // Explicitly copy strings to ensure they're captured properly
    std::string container = container_name;
    std::string blob = blob_name;
    
    return [client, container, blob, buffer, offset, length]() {
        using namespace Azure::Storage::Blobs;
        auto container_client = client->GetBlobContainerClient(container);
        auto blob_client = container_client.GetBlockBlobClient(blob);
        
        DownloadBlobToOptions download_options;
        download_options.Range = Azure::Core::Http::HttpRange();
        download_options.Range.Value().Offset = offset;
        download_options.Range.Value().Length = length;
        
        auto response = blob_client.DownloadTo(
            reinterpret_cast<uint8_t*>(buffer), length, download_options
        );
        
        // Verify the download size
        if (!response.Value.ContentRange.Length.HasValue() || 
            response.Value.ContentRange.Length.Value() != length) {
            throw std::runtime_error("Azure blob read size mismatch");
        }
    };
}

/**
 * An async wrapper for Azure Blob Storage client operations.
 * Uses ThreadPool to manage concurrent blob download operations.
 */
struct AsyncAzureClient
{
public:
    AsyncAzureClient(std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> client, unsigned max_pool_size);

    void DownloadBlobRangeAsync(
        const std::string& container_name,
        const std::string& blob_name,
        char* buffer,
        size_t offset,
        size_t length,
        CompletionCallback callback)
    {
        DownloadBlobFn downloadFn = createDownloadBlobFn(_client, container_name, blob_name, buffer, offset, length);

        DownloadBlobTask task{
            std::move(downloadFn),
            std::move(callback)
        };

        push_task(std::move(task));
    }

private:
    void push_task(DownloadBlobTask&& task);

    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> _client;
    utils::ThreadPool<DownloadBlobTask> _pool;
};

}; // namespace runai::llm::streamer::impl::azure
