load("//:rules.bzl", "runai_portable_so")

# Mock CUDA driver library for unit testing without a GPU.
# Built as libcuda.so.1 — the same name dlopen searches for — so the test
# binary's DT_RPATH finds it before the system driver on any machine.
runai_portable_so(
    name = "libcuda.so.1",
    srcs = ["libcuda_mock.cc"],
    ldscript = ":libcuda.ldscript",
    visibility = ["//visibility:public"],
)
