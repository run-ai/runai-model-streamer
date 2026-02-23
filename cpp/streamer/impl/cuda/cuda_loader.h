
#pragma once

#include <cuda.h>

namespace runai::llm::streamer::impl::cuda
{

// Subset of the CUDA driver API loaded at runtime from libcuda.so.1.
// We include cuda.h for correct type definitions (CUresult, CUdeviceptr, CUstream)
// so the compiler validates our function pointer signatures against the official ABI.
// The actual library is loaded via dlopen at runtime, so the built .so has no hard
// dependency on a specific libcuda.so version.
//
// Obtain the singleton via get(); returns nullptr if CUDA is unavailable at runtime.
struct CudaDriver
{
    // Releases the retained primary context on destruction (see ctx below).
    ~CudaDriver();

    // The primary CUDA context for device 0, retained once at load time.
    // Worker threads call cuCtxSetCurrent(ctx) on first use to make it current.
    // cuDevicePrimaryCtxRelease(0) is called in the destructor to balance the retain.
    CUcontext ctx = nullptr;

    CUresult (*cuStreamCreate)           (CUstream *, unsigned int flags);
    CUresult (*cuStreamDestroy)          (CUstream);
    CUresult (*cuStreamSynchronize)      (CUstream);
    CUresult (*cuMemAllocHost)           (void **, size_t);
    CUresult (*cuMemFreeHost)            (void *);
    CUresult (*cuMemcpyHtoDAsync)        (CUdeviceptr dst, const void * src, size_t count, CUstream);
    CUresult (*cuDevicePrimaryCtxRelease)(CUdevice device);
    CUresult (*cuCtxSetCurrent)          (CUcontext);

    // Returns the process-wide singleton, or nullptr if libcuda.so could not be loaded.
    static const CudaDriver * get();
};

} // namespace runai::llm::streamer::impl::cuda
