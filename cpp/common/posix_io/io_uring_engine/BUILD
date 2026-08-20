load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "io_uring_engine",
    deps = [
        "//common/exception",
        "//common/posix_io/completion_mapper",
        "//common/posix_io/io_engine",
        "//common/response_code",
        "//utils/logging",
        "@uring//:uring",
    ],
)

runai_cc_test(
    name = "io_uring_engine_test",
    srcs = ["io_uring_engine_test.cc"],
    deps = [
        ":io_uring_engine",
        "//common/posix_io/io_uring_probe",
        "//utils/random",
        "//utils/temp/env",
        "//utils/temp/file",
    ],
)
