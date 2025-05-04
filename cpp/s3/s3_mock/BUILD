load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamers3.so",
    srcs = ["s3_mock.h",
            "s3_mock.cc",
    ],
    deps = [
        "//common/response_code",
        "//common/path",
        "//common/s3_credentials",
        "//common/range",
        "//utils/env",
        "//utils/random",
        "//utils/logging",
    ],
    ldscript = ":s3_mock.ldscript",
    visibility= ["//visibility:public"],
)
