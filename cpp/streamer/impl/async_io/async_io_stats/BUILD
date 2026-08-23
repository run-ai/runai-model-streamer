load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "async_io_stats",
    deps = [
        "//common/posix_io/strategy",
        "//common/submission",
    ],
)

runai_cc_test(
    name = "async_io_stats_test",
    srcs = ["async_io_stats_test.cc"],
    deps = [
        ":async_io_stats",
    ],
)
