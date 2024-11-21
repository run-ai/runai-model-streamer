load("//:rules.bzl", "runai_cc_binary")

runai_cc_binary(
    name = "verify_streamer",
    srcs = [
        "main.cc",
    ],
    deps = [
        "//streamer",
        "//utils/random",
        "//utils/fd",
        "//utils/strings",
    ],
    linkopts = ["-lpthread", "-Wl,-rpath,$$ORIGIN"],
)
