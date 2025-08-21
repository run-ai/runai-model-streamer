load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "client_configuration",
    deps = [
        "@google_cloud_cpp//:storage",
        "//common/exception",
        "//common/response_code",
        "//common/storage_uri",
        "//utils/env",
        "//utils/logging",
    ],
)
