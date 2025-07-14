load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamers3.so",
    srcs = ["s3.h",
            "s3.cc",
    ],
    deps = [
        "//s3/client_mgr",
        "//s3/s3_init",
    ],
    ldscript = ":s3.ldscript",
)
