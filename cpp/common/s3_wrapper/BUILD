load("//:rules.bzl", "runai_cc_auto_library",  "runai_cc_test")

runai_cc_auto_library(
    name = "s3_wrapper",
    deps = [
        "//common/storage_uri",
        "//common/response",
        "//common/range",
        "//common/exception",
        "//utils/dylib",
        "//utils/env",
        "//utils/semver",
    ],
)

runai_cc_test(
    name = "s3_wrapper_test",
    srcs = ["s3_wrapper_test.cc"],
    deps = [
        ":s3_wrapper",
        "//s3/s3_mock:libstreamers3.so",
        "//utils/random",
    ],
    data = ["//s3/s3_mock:libstreamers3.so"],
    linkopts = [
        "-Wl,-rpath,$$ORIGIN/../../s3/s3_mock",
    ],
)
