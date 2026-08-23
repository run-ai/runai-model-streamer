load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "alignment",
    deps = [
        "//common/posix_io/io_engine",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "alignment_test",
    srcs = ["alignment_test.cc"],
    deps = [
        ":alignment",
    ],
)
