load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "config",
    deps = [
        "//utils/env",
        "//utils/logging",
        "//common/s3_wrapper",
    ]
)

runai_cc_test(
    name = "config_test",
    srcs = ["config_test.cc"],
    deps = [":config",
            "//utils/random",
            "//utils/temp/env",
    ],
)
