load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "strategy_resolver",
    deps = [
        "//common/exception",
        "//common/posix_io/io_uring_probe",
        "//common/posix_io/libaio_probe",
        "//common/posix_io/strategy",
        "//common/response_code",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "strategy_resolver_test",
    srcs = ["strategy_resolver_test.cc"],
    deps = [
        ":strategy_resolver",
        "//utils/temp/env",
    ],
)
