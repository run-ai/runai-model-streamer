load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "client",
    deps = [
        "//s3/client_configuration",
        "//common/backend_api/response",
        "//common/response_code",
        "//common/shared_queue",
        "//common/range",
        "//common/s3_credentials",
        "//common/s3_wrapper",
        "//common/path",
        "//common/exception",
        "//utils/env",
        "//utils/fd",
    ],
)
