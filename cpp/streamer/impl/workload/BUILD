load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "workload",
    deps = [
        "//utils/logging",
        "//common/obj_store_wrapper",
        "//common/exception",
        "//common/response_code",
        "//streamer/impl/batch",
        "//streamer/impl/obj_store",
        "//streamer/impl/reader",
    ],
)

runai_cc_test(
    name = "workload_test",
    srcs = ["workload_test.cc"],
    deps = [":workload",
            "//streamer/impl/obj_store",
            "//streamer/impl/assigner",
            "//streamer/impl/batches",
            "//utils/random",
            "//utils/thread",
            "//utils/semaphore",
            "//utils/temp/file",],
    data = ["//obj_store/obj_store_mock:libstreamerobj_store.so"],
    linkopts = [
        "-Wl,-rpath,$$ORIGIN/../../../obj_store/obj_store_mock",
    ],
)