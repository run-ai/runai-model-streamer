load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "misc",
    deps = [
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "misc_test",
    srcs = ["misc_test.cc"],
    deps = [
        ":misc",
    ],
)
