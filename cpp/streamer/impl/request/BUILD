load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "request",
    deps = [
        "//common/response_code",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "request_test",
    srcs = ["request_test.cc"],
    deps = [":request",
            "//utils/threadpool",
            "//utils/random",
    ],
)
