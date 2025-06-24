load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "client_mgr",
    deps = [
        "//gcs/client",
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
