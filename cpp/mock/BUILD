load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamer-mock.so",
    srcs = ["streamer-mock.cc"],
    deps = [
        "//utils/fd",
    ],
    ldscript = "//streamer:streamer.ldscript",
)
