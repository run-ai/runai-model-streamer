load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "obj_store",
    deps = [
        "//common/obj_store_wrapper",
        "//common/exception",
        "//streamer/impl/reader",
        "//streamer/impl/config",
        "//utils/logging",
    ],
)
