load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "backend_pools",
    deps = [
        "//common/response_code",
        "//utils/env",
        "//utils/logging",
        "//utils/threadpool",
        "//streamer/impl/workload",
    ],
)

runai_cc_test(
    name = "backend_pools_test",
    srcs = ["backend_pools_test.cc"],
    deps = [
        ":backend_pools",
        "//utils/temp/env",
    ],
)
