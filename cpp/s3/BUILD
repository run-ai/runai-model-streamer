load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamers3.so",
    srcs = ["s3.h",
            "s3.cc",
    ],
    deps = [
        "//utils/strings",
        "//s3/client_mgr",
    ],
    ldscript = ":s3.ldscript",
)
