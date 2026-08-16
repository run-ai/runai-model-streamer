load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

# A TEST-ONLY target, and its own Bazel package for that reason: glob() does not cross package
# boundaries, so //common/posix_io's source glob cannot pull the mock into the shipped library.
# Nothing under //streamer may depend on this.
runai_cc_auto_library(
    name = "mock",
    deps = [
        "//common/posix_io",
        "//utils/logging",
    ],
    testonly = True,
)

runai_cc_test(
    name = "mock_io_engine_test",
    srcs = ["mock_io_engine_test.cc"],
    deps = [
        ":mock",
    ],
)
