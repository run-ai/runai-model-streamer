load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "engine_factory",
    deps = [
        "//common/exception",
        "//common/posix_io/io_engine",
        "//common/posix_io/io_uring_engine",
        "//common/posix_io/io_uring_probe",
        "//common/posix_io/strategy",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "engine_factory_test",
    srcs = ["engine_factory_test.cc"],
    deps = [
        ":engine_factory",
    ],
)
