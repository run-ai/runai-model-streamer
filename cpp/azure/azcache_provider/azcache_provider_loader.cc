#include "azure/azcache_provider/azcache_provider_loader.h"

#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <algorithm>

#include "utils/logging/logging.h"
#include "utils/env/env.h"

namespace runai::llm::streamer::impl::azure
{

namespace
{

// Known cache provider package/library names for auto-discovery
constexpr const char* CACHE_PROVIDER_PACKAGE = "py_tachyon_client";
constexpr const char* CACHE_PROVIDER_LIB = "libStorageDirect.so";

/**
 * Use dladdr on a symbol known to live in libstreamerazure.so to locate
 * the site-packages directory, then look for a known cache provider .so.
 *
 * Layout: <site-packages>/runai_model_streamer/libstreamer/lib/libstreamerazure.so
 *         <site-packages>/<CACHE_PROVIDER_PACKAGE>/<CACHE_PROVIDER_LIB>
 */
std::string autodiscover_cache_lib()
{
    // Use dladdr on a function in this translation unit to find where
    // libstreamerazure.so is loaded from
    Dl_info info;
    auto fn_ptr = reinterpret_cast<void*>(&autodiscover_cache_lib);
    if (!dladdr(fn_ptr, &info) || !info.dli_fname)
    {
        LOG(DEBUG) << "AzCacheProvider: dladdr failed — cannot auto-discover";
        return {};
    }

    std::error_code ec;
    auto azure_so = std::filesystem::weakly_canonical(info.dli_fname, ec);
    if (ec)
    {
        LOG(DEBUG) << "AzCacheProvider: canonical path failed for " << info.dli_fname;
        return {};
    }

    // libstreamerazure.so → lib/ → libstreamer/ → runai_model_streamer/ → site-packages/
    auto site_packages = azure_so.parent_path().parent_path().parent_path().parent_path();
    auto candidate = site_packages / CACHE_PROVIDER_PACKAGE / CACHE_PROVIDER_LIB;

    if (std::filesystem::exists(candidate, ec) && !ec)
    {
        return candidate.string();
    }

    LOG(DEBUG) << "AzCacheProvider: auto-discovery checked " << candidate.string() << " — not found";
    return {};
}

bool is_disabled_value(const std::string& val)
{
    std::string lower = val;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    return lower == "0" || lower == "false" || lower == "disabled" || lower == "none" || lower == "no";
}

} // anonymous namespace

AzCacheProviderLoader& AzCacheProviderLoader::instance()
{
    static AzCacheProviderLoader instance;
    return instance;
}

AzCacheProviderLoader::AzCacheProviderLoader()
    : _lib_handle(nullptr),
      _cache_read(nullptr),
      _enabled(false)
{
    // Master kill switch — check RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED first
    std::string enabled_val;
    if (utils::try_getenv<std::string>(RUNAI_AZURE_CACHE_ENABLED_ENV, enabled_val))
    {
        if (is_disabled_value(enabled_val))
        {
            LOG(INFO) << "AzCacheProvider: disabled via " << RUNAI_AZURE_CACHE_ENABLED_ENV << "=" << enabled_val;
            return;
        }
    }

    // Determine library path: explicit override or auto-discovery
    std::string lib_path;
    if (utils::try_getenv<std::string>(RUNAI_AZURE_CACHE_LIB_ENV, lib_path))
    {
        if (is_disabled_value(lib_path))
        {
            LOG(INFO) << "AzCacheProvider: explicitly disabled via " << RUNAI_AZURE_CACHE_LIB_ENV << "=" << lib_path;
            return;
        }
    }
    else
    {
        // Env var not set — try auto-discovery
        lib_path = autodiscover_cache_lib();
        if (lib_path.empty())
        {
            LOG(DEBUG) << "AzCacheProvider: no cache provider found — cache disabled";
            return;
        }
        LOG(INFO) << "AzCacheProvider: auto-discovered cache library: " << lib_path;
    }

    _lib_path = lib_path;
    LOG(INFO) << "AzCacheProvider: loading cache library: " << _lib_path;

    _lib_handle = dlopen(_lib_path.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!_lib_handle)
    {
        LOG(WARNING) << "AzCacheProvider: dlopen failed for '" << _lib_path << "': " << dlerror();
        return;
    }

    _cache_read = reinterpret_cast<blob_read_fn>(
        dlsym(_lib_handle, BLOB_READ_SYMBOL));
    if (!_cache_read)
    {
        LOG(WARNING) << "AzCacheProvider: dlsym failed for '" << BLOB_READ_SYMBOL
                     << "': " << dlerror();
        dlclose(_lib_handle);
        _lib_handle = nullptr;
        return;
    }

    _enabled = true;
    LOG(INFO) << "AzCacheProvider: cache provider loaded successfully from " << _lib_path;
}

AzCacheProviderLoader::~AzCacheProviderLoader()
{
    // Intentionally do NOT dlclose — at static destruction time the loaded
    // library may have already torn down its own statics, leading to
    // use-after-free.  The OS reclaims everything on process exit.
}

bool AzCacheProviderLoader::read(
    const std::string& account,
    const std::string& container,
    const std::string& blob,
    char* buffer,
    size_t offset,
    size_t length)
{
    if (!_enabled)
    {
        return false;
    }

    char* error_string = nullptr;
    ssize_t bytes_read = _cache_read(
        account.c_str(), container.c_str(), blob.c_str(),
        buffer, offset, length, &error_string);

    if (bytes_read < 0 || static_cast<size_t>(bytes_read) != length)
    {
        if (error_string)
        {
            LOG(ERROR) << "AzCacheProvider: cache read failed for "
                       << account << "/" << container << "/" << blob
                       << " offset=" << offset << " length=" << length
                       << ": " << error_string;
            free(error_string);
        }
        else
        {
            LOG(ERROR) << "AzCacheProvider: cache read failed for "
                       << account << "/" << container << "/" << blob
                       << " offset=" << offset << " length=" << length;
        }
        return false;
    }

    LOG(SPAM) << "AzCacheProvider: cache read " << length << " bytes from "
              << account << "/" << container << "/" << blob << " offset=" << offset;
    return true;
}

} // namespace runai::llm::streamer::impl::azure
