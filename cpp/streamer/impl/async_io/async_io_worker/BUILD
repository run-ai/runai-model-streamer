load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "async_io_worker",
    deps = [
        "//common/posix_io/io_engine",
        "//common/posix_io/strategy",
        "//common/exception",
        "//streamer/impl/async_io/async_io_settings",
        "//streamer/impl/async_io/inflight_chunks",
        "//streamer/impl/batch",
        "//streamer/impl/workload",
        "//utils/capacity_worker",
        "//utils/logging",
    ],
)
