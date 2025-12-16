load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "obj_store_init",
    deps = [
        "@aws",
        "//utils/env",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "obj_store_init_test",
    srcs = ["obj_store_init_test.cc"],
    deps = [":obj_store_init"],
)