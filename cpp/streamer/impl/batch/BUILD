load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "batch",
    deps = [
        "//common/responder",
        "//common/shared_queue",
        "//streamer/impl/config",
        "//streamer/impl/cuda:cuda_loader",
        "//streamer/impl/file",
        "//streamer/impl/s3",
        "//streamer/impl/task",
    ],
)

runai_cc_test(
    name = "batch_cuda_test",
    srcs = ["batch_cuda_test.cc"],
    deps = [
        ":batch",
        "//utils/random",
        "//utils/temp/file",
        "//common/s3_wrapper",
        "//cuda/cuda_mock:libcuda.so.1",
    ],
    data = ["//cuda/cuda_mock:libcuda.so.1"],
    linkopts = [
        # DT_RPATH (not RUNPATH) is searched before LD_LIBRARY_PATH, so the mock
        # wins even on GPU machines where LD_LIBRARY_PATH includes the real CUDA.
        "-Wl,-rpath=$$ORIGIN/../../../cuda/cuda_mock,--disable-new-dtags",
    ],
)

runai_cc_test(
    name = "batch_test",
    srcs = ["batch_test.cc"],
    deps = [":batch",
            "//streamer/impl/file",
            "//streamer/impl/workload",
            "//utils/random",
            "//utils/temp/file",
            "//utils/thread",
            "//utils/dylib",
            "//utils/scope_guard",
            "//common/s3_wrapper",
            "//s3/s3_mock:libstreamers3.so",
    ],
    data = ["//s3/s3_mock:libstreamers3.so"],
    linkopts = [
        "-Wl,-rpath,$$ORIGIN/../../../s3/s3_mock",
    ],
)
