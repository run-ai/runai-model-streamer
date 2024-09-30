load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "thread",
    deps = [
        "//utils/logging",
    ]
)

runai_cc_test(
    name = "thread_test",
    srcs = ["thread_test.cc"],
    deps = [
        ":thread",
        "//utils/logging",
        "//utils/temp/env",
    ],
)
