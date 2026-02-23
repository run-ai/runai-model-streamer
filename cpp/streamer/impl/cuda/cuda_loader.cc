#include "streamer/impl/cuda/cuda_loader.h"

#include <dlfcn.h>

#include "utils/logging/logging.h"

namespace runai::llm::streamer::impl::cuda
{

CudaDriver::~CudaDriver()
{
    if (ctx != nullptr && cuDevicePrimaryCtxRelease != nullptr)
    {
        cuDevicePrimaryCtxRelease(0);
    }
}

namespace
{

template<typename T>
T load_sym(void * handle, const char * versioned_name, const char * fallback_name = nullptr)
{
    auto fn = reinterpret_cast<T>(dlsym(handle, versioned_name));
    if (!fn && fallback_name)
    {
        fn = reinterpret_cast<T>(dlsym(handle, fallback_name));
    }
    return fn;
}

CudaDriver load()
{
    // Try the stable SONAME first, then the unversioned symlink as fallback.
    void * handle = nullptr;
    for (const char * lib : {"libcuda.so.1", "libcuda.so"})
    {
        handle = dlopen(lib, RTLD_LAZY | RTLD_LOCAL);
        if (handle)
        {
            break;
        }
    }

    if (!handle)
    {
        LOG(INFO) << "[RunAI Streamer] CUDA driver library not found; CUDA streaming disabled";
        return {};
    }

    CudaDriver d{};

    // For driver API functions that were given a _v2 suffix in CUDA 4.0 when
    // CUdeviceptr was widened to 64 bits, prefer the versioned symbol on 64-bit
    // builds and fall back to the unversioned symbol for older drivers.
    d.cuStreamCreate           = load_sym<decltype(d.cuStreamCreate)>           (handle, "cuStreamCreate");
    d.cuStreamDestroy          = load_sym<decltype(d.cuStreamDestroy)>          (handle, "cuStreamDestroy_v2",  "cuStreamDestroy");
    d.cuStreamSynchronize      = load_sym<decltype(d.cuStreamSynchronize)>      (handle, "cuStreamSynchronize");
    d.cuMemAllocHost           = load_sym<decltype(d.cuMemAllocHost)>           (handle, "cuMemAllocHost_v2",   "cuMemAllocHost");
    d.cuMemFreeHost            = load_sym<decltype(d.cuMemFreeHost)>            (handle, "cuMemFreeHost");
    d.cuMemcpyHtoDAsync        = load_sym<decltype(d.cuMemcpyHtoDAsync)>        (handle, "cuMemcpyHtoDAsync_v2","cuMemcpyHtoDAsync");
    d.cuDevicePrimaryCtxRelease = load_sym<decltype(d.cuDevicePrimaryCtxRelease)>(handle, "cuDevicePrimaryCtxRelease");
    d.cuCtxSetCurrent          = load_sym<decltype(d.cuCtxSetCurrent)>          (handle, "cuCtxSetCurrent");

    if (!d.cuStreamCreate || !d.cuStreamDestroy || !d.cuStreamSynchronize ||
        !d.cuMemAllocHost || !d.cuMemFreeHost   || !d.cuMemcpyHtoDAsync  ||
        !d.cuDevicePrimaryCtxRelease || !d.cuCtxSetCurrent)
    {
        LOG(WARNING) << "[RunAI Streamer] Not all CUDA driver symbols could be resolved; CUDA streaming disabled";
        return {};
    }

    // Retain the primary context for device 0 once per process.
    // Worker threads make it current via cuCtxSetCurrent(ctx) on first use.
    // The matching cuDevicePrimaryCtxRelease(0) is called in ~CudaDriver().
    auto cuDevicePrimaryCtxRetain =
        load_sym<CUresult(*)(CUcontext *, CUdevice)>(handle, "cuDevicePrimaryCtxRetain");
    if (!cuDevicePrimaryCtxRetain || cuDevicePrimaryCtxRetain(&d.ctx, 0) != CUDA_SUCCESS)
    {
        LOG(WARNING) << "[RunAI Streamer] Could not retain CUDA primary context; CUDA streaming disabled";
        return {};
    }

    LOG(INFO) << "[RunAI Streamer] CUDA driver loaded successfully";
    return d;
}

} // anonymous namespace

const CudaDriver * CudaDriver::get()
{
    static const CudaDriver driver   = load();
    static const bool       available = driver.cuStreamCreate != nullptr;
    return available ? &driver : nullptr;
}

} // namespace runai::llm::streamer::impl::cuda
