load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "env",
    deps = [
        "//utils/logging",
        "//utils/random",
    ],
)

runai_cc_test(
    name = "temp_env_test",
    srcs = ["env_test.cc"],
    deps = [
        ":env",
        "//utils/env",
    ],
)
