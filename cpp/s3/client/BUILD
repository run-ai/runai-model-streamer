load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "client",
    deps = [
        "//s3/client_configuration",
        "//common/response_code",
        "//common/response",
        "//common/responder",
        "//common/range",
        "//common/s3_credentials",
        "//common/path",
        "//common/exception",
        "//utils/env",
        "//utils/fd",
    ],
)
