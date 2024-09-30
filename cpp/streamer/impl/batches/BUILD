load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "batches",
    deps = [
        "//common/s3_wrapper",
        "//streamer/impl/config",
        "//streamer/impl/batch",
        "//common/exception",
        "//common/responder",
        "//streamer/impl/file",
        "//streamer/impl/s3",
        "//utils/logging",
    ],
    linkopts = [ "-lm" ]
)

runai_cc_test(
    name = "batches_test",
    srcs = ["batches_test.cc"],
    deps = [
        ":batches",
        "//utils/random",
        "//utils/temp/file",
    ],
)
