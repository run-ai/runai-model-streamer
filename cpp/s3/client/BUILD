load("//:rules.bzl", "runai_cc_auto_library")

runai_cc_auto_library(
    name = "client",
    deps = [
        "//obj_store/client_configuration",
        "//common/backend_api/response",
        "//common/response_code",
        "//common/shared_queue",
        "//common/range",
        "//common/obj_store_wrapper",
        "//common/exception",
        "//common/storage_uri",
        "//common/client_mgr",
        "//common/backend_api/object_storage",
        "//utils/env",
        "//utils/fd",
    ],
)
