load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")


runai_cc_auto_library(
    name = "file",
    deps = [
        "//utils/logging",
        "//utils/random",
        "//utils/fd",
    ],
)

runai_cc_test(
    name = "file_test",
    srcs = ["file_test.cc"],
    deps = [
        ":file",
    ],
)
