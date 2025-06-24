load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamers3gcs.so",
    srcs = [
        "gcs.h",
        "gcs.cc",
    ],
    deps = [
        "//gcs/client_mgr",
    ],
    ldscript = ":gcs.ldscript",
)
