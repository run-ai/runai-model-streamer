load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "fd",
    deps = [
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "fd_test",
    srcs = ["fd_test.cc"],
    deps = [
        ":fd",
        "//utils/random",
        "//utils/temp/file",
    ],
)
