load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamer-mock.so",
    srcs = ["streamer-mock.cc"],
    deps = [
        "//utils/fd",
        "//common/response_code",
    ],
    ldscript = "//streamer:streamer.ldscript",
)
