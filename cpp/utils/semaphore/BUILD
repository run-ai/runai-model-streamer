load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "semaphore",
    deps = ["//utils/logging"],
)

runai_cc_test(
    name = "semaphore_test",
    srcs = ["semaphore_test.cc"],
    deps = [
        ":semaphore",
        "//utils/thread",
        "//utils/random",
    ],
)
