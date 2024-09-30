load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "client_mgr",
    deps = [
        "//s3/s3_init",
        "//s3/client",
    ],
)
runai_cc_test(
    name = "client_mgr_test",
    srcs = ["client_mgr_test.cc"],
    deps = [
        ":client_mgr",
        "//utils/random",
        ],
)
