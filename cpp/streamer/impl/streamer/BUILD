load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "streamer",
    deps = [
        "//streamer/impl/config",
        "//streamer/impl/batches",
        "//common/responder",
        "//utils/threadpool",
        "//utils/fdlimit",
    ],
)

runai_cc_test(
    name = "streamer_test",
    srcs = ["streamer_test.cc"],
    deps = [
        ":streamer",
        "//utils/random",
        "//utils/fd",
        "//utils/temp/file",
    ],
)
