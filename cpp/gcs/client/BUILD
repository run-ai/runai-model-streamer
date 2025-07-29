load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "client",
    deps = [
        "@google_cloud_cpp//:storage",
        "//gcs/client_configuration",
        "//common/backend_api/response",
        "//common/response_code",
        "//common/shared_queue",
        "//common/range",
        "//common/client_mgr",
        "//common/s3_wrapper",
        "//common/exception",
        "//common/storage_uri",
        "//common/backend_api/object_storage",
        "//utils/env",
        "//utils/fd",
    ],
)
