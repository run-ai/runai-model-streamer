load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "async_gcs_client",
    deps = [
        "@google_cloud_cpp//:storage",
        "//utils/threadpool",
    ],
)
