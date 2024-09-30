load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "batch",
    deps = [
        "//common/responder",
        "//streamer/impl/config",
        "//streamer/impl/file",
        "//streamer/impl/s3",
        "//streamer/impl/task",
    ],
)

runai_cc_test(
    name = "batch_test",
    srcs = ["batch_test.cc"],
    deps = [":batch",
            "//streamer/impl/file",
            "//utils/random",
            "//utils/temp/file",
    ],
)
