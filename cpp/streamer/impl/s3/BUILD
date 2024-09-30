load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "s3",
    deps = [
        "//common/s3_wrapper",
        "//common/exception",
        "//streamer/impl/reader",
        "//streamer/impl/config",
        "//utils/logging",
    ],
)
