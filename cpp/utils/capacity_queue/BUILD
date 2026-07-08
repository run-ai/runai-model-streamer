load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "capacity_queue",
)

runai_cc_test(
    name = "capacity_queue_test",
    srcs = ["capacity_queue_test.cc"],
    deps = [
        ":capacity_queue",
    ],
)
