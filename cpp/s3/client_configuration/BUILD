load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "client_configuration",
    deps = [
        "@aws",
        "//common/storage_uri",
        "//common/backend_api/object_storage",
        "//utils/env",
        "//utils/logging",
    ],
)
