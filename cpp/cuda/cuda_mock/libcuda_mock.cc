// Mock implementation of the CUDA driver API for unit testing without a GPU.
//
// cuMemcpyHtoDAsync treats CUdeviceptr as a plain host pointer and performs
// a memcpy, so tests can verify data correctness without any GPU hardware.
// All stream and context operations are no-ops.
//
// This library is built as libcuda.so.1 and injected via DT_RPATH in test
// binaries, taking precedence over the real driver even on GPU machines.

#include <cstdlib>
#include <cstring>

// ABI-compatible type definitions — no CUDA SDK required.
typedef int                  CUresult;
typedef unsigned long long   CUdeviceptr;
typedef int                  CUdevice;
struct CUstream_st;
typedef struct CUstream_st * CUstream;
struct CUctx_st;
typedef struct CUctx_st *    CUcontext;

#define CUDA_SUCCESS             0
#define CUDA_ERROR_OUT_OF_MEMORY 2

extern "C"
{

// --- Pinned host memory: plain malloc/free ---

CUresult cuMemAllocHost_v2(void ** pp, size_t bytesize)
{
    *pp = malloc(bytesize);
    return *pp ? CUDA_SUCCESS : CUDA_ERROR_OUT_OF_MEMORY;
}

CUresult cuMemFreeHost(void * p)
{
    free(p);
    return CUDA_SUCCESS;
}

// --- Streams: non-null sentinel so CudaDriver::get() passes all null-checks ---

static int _stream_sentinel;

CUresult cuStreamCreate(CUstream * phStream, unsigned int /*flags*/)
{
    *phStream = reinterpret_cast<CUstream>(&_stream_sentinel);
    return CUDA_SUCCESS;
}

CUresult cuStreamDestroy_v2(CUstream /*hStream*/)
{
    return CUDA_SUCCESS;
}

CUresult cuStreamSynchronize(CUstream /*hStream*/)
{
    return CUDA_SUCCESS;
}

// --- Context: non-null sentinel ---

static int _ctx_sentinel;

CUresult cuDevicePrimaryCtxRetain(CUcontext * pctx, CUdevice /*dev*/)
{
    *pctx = reinterpret_cast<CUcontext>(&_ctx_sentinel);
    return CUDA_SUCCESS;
}

CUresult cuDevicePrimaryCtxRelease(CUdevice /*dev*/)
{
    return CUDA_SUCCESS;
}

CUresult cuCtxSetCurrent(CUcontext /*ctx*/)
{
    return CUDA_SUCCESS;
}

// --- Core copy: CUdeviceptr is uint64; treat as host pointer and memcpy ---

CUresult cuMemcpyHtoDAsync_v2(CUdeviceptr  dstDevice,
                               const void * srcHost,
                               size_t       byteCount,
                               CUstream     /*hStream*/)
{
    memcpy(reinterpret_cast<void *>(dstDevice), srcHost, byteCount);
    return CUDA_SUCCESS;
}

} // extern "C"
