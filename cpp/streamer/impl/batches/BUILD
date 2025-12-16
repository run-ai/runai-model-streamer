load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "batches",
    deps = [
        "//streamer/impl/config",
        "//streamer/impl/batch",
        "//common/obj_store_wrapper",
        "//common/exception",
        "//common/responder",
        "//streamer/impl/file",
        "//streamer/impl/assigner/file_read_task",
        "//streamer/impl/obj_store",
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
        "//streamer/impl/assigner",
    ],
)
