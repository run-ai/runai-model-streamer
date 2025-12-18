load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "shared_queue",
    deps = [
        "//utils/semaphore",
        "//utils/logging",
        "//common/response_code",
    ],
)

runai_cc_test(
    name = "shared_queue_test",
    srcs = ["shared_queue_test.cc"],
    deps = [":shared_queue",
            "//utils/threadpool",
            "//utils/thread",
            "//utils/random",
            "//common/response",
    ],
)
