load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "batch",
    deps = [
        "//common/responder",
        "//common/shared_queue",
        "//streamer/impl/config",
        "//streamer/impl/file",
        "//streamer/impl/obj_store",
        "//streamer/impl/task",
    ],
)

runai_cc_test(
    name = "batch_test",
    srcs = ["batch_test.cc"],
    deps = [":batch",
            "//streamer/impl/file",
            "//streamer/impl/workload",
            "//utils/random",
            "//utils/temp/file",
            "//utils/thread",
            "//utils/dylib",
            "//utils/scope_guard",
            "//common/obj_store_wrapper",
            "//obj_store/obj_store_mock:libstreamerobj_store.so",
    ],
    data = ["//obj_store/obj_store_mock:libstreamerobj_store.so"],
    linkopts = [
        "-Wl,-rpath,$$ORIGIN/../../../obj_store/obj_store_mock",
    ],
)
