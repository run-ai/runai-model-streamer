# Headers-only target for the CUDA driver API.
# Provides cuda.h and related headers for compile-time type checking.
# The actual libcuda.so is NOT linked here — it is loaded at runtime via dlopen,
# so the built binary has no hard dependency on any specific CUDA version.
cc_library(
    name = "cuda_headers",
    hdrs = glob([
        "include/cuda.h",
        "include/cuda_runtime.h",
        "include/driver_types.h",
        "include/vector_types.h",
    ], allow_empty = True),
    includes = ["include"],
    visibility = ["//visibility:public"],
)
