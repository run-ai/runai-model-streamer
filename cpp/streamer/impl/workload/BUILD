load("//:rules.bzl", "runai_cc_auto_library")

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