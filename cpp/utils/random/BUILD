load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "random",
    deps = [
        "//utils/logging",
        "//utils/scope_guard",
    ],
)

runai_cc_test(
    name = "random_test",
    srcs = ["random_test.cc"],
    deps = [
        ":random",
    ],
)
