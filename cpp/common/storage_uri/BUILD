load("//:rules.bzl", "runai_cc_auto_library",  "runai_cc_test")

runai_cc_auto_library(
    name = "storage_uri",
    deps = [
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "storage_uri_test",
    srcs = ["storage_uri_test.cc"],
    deps = [
        ":storage_uri",
        "//utils/random",
    ],
)
