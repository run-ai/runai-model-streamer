cc_library(
    name = "async_azure_client",
    srcs = ["async_azure_client.cc"],
    hdrs = ["async_azure_client.h"],
    visibility = ["//azure:__subpackages__"],
    deps = [
        "//common/response_code",
        "//utils/threadpool",
        "//utils/logging",
        "@azure//:azure",
    ],
)
