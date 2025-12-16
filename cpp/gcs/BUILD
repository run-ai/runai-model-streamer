load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamergcs.so",
    srcs = [
        "gcs.h",
        "gcs.cc",
    ],
    deps = [
        "//common/client_mgr",
        "//common/range",
        "//gcs/client",
    ],
    ldscript = ":gcs.ldscript",
)
