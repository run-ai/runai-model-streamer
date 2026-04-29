#pragma once

#include <atomic>
#include <mutex>
#include <string>

#include "azure/azcache_provider/runai_azcache_provider.h"

namespace runai::llm::streamer::impl::azure
{

/**
 * AzCacheProviderLoader dynamically loads a cache provider .so at runtime
 * via dlopen/dlsym.
 *
 * Auto-discovers the cache library in Python site-packages (e.g., tachyon_client).
 * Can be disabled with RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED=false.
 * Can be overridden with RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_LIB=/path/to.so.
 *
 * Thread-safe singleton — initialization happens once on first access.
 */
class AzCacheProviderLoader
{
public:
    static AzCacheProviderLoader& instance();

    bool is_enabled() const { return _enabled.load(std::memory_order_relaxed); }

    /**
     * Read blob data through the cache provider.
     *
     * @param account    Azure Storage account name
     * @param container  Azure container name
     * @param blob       Blob path within the container
     * @param buffer     Destination buffer (caller-allocated)
     * @param offset     Byte offset within the blob
     * @param length     Number of bytes to read
     * @return true on success, false on error
     */
    bool read(const std::string& account,
              const std::string& container,
              const std::string& blob,
              char* buffer,
              size_t offset,
              size_t length);

    AzCacheProviderLoader(const AzCacheProviderLoader&) = delete;
    AzCacheProviderLoader& operator=(const AzCacheProviderLoader&) = delete;

private:
    AzCacheProviderLoader();
    ~AzCacheProviderLoader();

    void* _lib_handle;
    blob_read_fn _cache_read;
    std::atomic<bool> _enabled;
    std::string _lib_path;
};

} // namespace runai::llm::streamer::impl::azure
