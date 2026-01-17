load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamerazure.so",
    srcs = [
        "azure.h",
        "azure.cc",
    ],
    deps = [
        "//common/client_mgr",
        "//common/range",
        "//common/exception",
        "//utils/semver",
        "//azure/client",
    ],
    ldscript = ":azure.ldscript",
)
