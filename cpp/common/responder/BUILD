load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "responder",
    deps = [
        "//utils/semaphore",
        "//utils/logging",
        "//common/response",
    ],
)

runai_cc_test(
    name = "responder_test",
    srcs = ["responder_test.cc"],
    deps = [":responder",
            "//utils/threadpool",
            "//utils/random",
    ],
)
