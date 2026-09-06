load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "fs_parallelism",
    deps = [
        "//common/exception",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "fs_parallelism_test",
    srcs = ["fs_parallelism_test.cc"],
    deps = [
        ":fs_parallelism",
        "//utils/random",
    ],
)
