cc_library(
    name = "client",
    hdrs = [
        "client.h",
    ],
    srcs = [
        "client.cc",
    ],
    deps = [
        "@azure//:azure",
        "//common/backend_api/response",
        "//common/backend_api/object_storage",
        "//common/client_mgr",
        "//common/storage_uri",
        "//common/shared_queue",
        "//common/range",
        "//common/exception",
        "//utils/logging",
        "//utils/env",
        "//azure/client_configuration",
    ],
    visibility = ["//visibility:public"],
)
