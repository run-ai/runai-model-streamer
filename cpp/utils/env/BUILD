load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "env",
    deps = [
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "env_test",
    srcs = ["env_test.cc"],
    deps = [
        ":env",
        "//utils/temp/env",
    ],
)
