load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "workload",
    deps = [
        "//utils/logging",
        "//common/s3_wrapper",
        "//common/exception",
        "//common/response_code",
        "//streamer/impl/batch",
        "//streamer/impl/s3",
        "//streamer/impl/reader",
    ],
)

runai_cc_test(
    name = "workload_test",
    srcs = ["workload_test.cc"],
    deps = [":workload",
            "//streamer/impl/s3",
            "//streamer/impl/assigner",
            "//streamer/impl/batches",
            "//utils/random",
            "//utils/thread",
            "//utils/semaphore",
            "//utils/temp/file",],
    data = ["//s3/s3_mock:libstreamers3.so"],
    linkopts = [
        "-Wl,-rpath,$$ORIGIN/../../../s3/s3_mock",
    ],
)