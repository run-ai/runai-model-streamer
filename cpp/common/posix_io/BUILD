load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "posix_io",
    deps = [
        "//common/exception",
        "//common/response_code",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "strategy_test",
    srcs = ["strategy_test.cc"],
    deps = [
        ":posix_io",
    ],
)

runai_cc_test(
    name = "io_engine_test",
    srcs = ["io_engine_test.cc"],
    deps = [
        ":posix_io",
    ],
)
