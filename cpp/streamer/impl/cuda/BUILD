load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "cuda_loader",
    deps = [
        "@cuda//:cuda_headers",
        "//utils/logging",
    ],
    # dlopen is used at runtime to load libcuda.so — no link-time dependency on CUDA.
    linkopts = ["-ldl"],
)
