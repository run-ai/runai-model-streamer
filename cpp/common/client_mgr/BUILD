load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "client_mgr",
    deps = [
        "//common/backend_api/object_storage",
    ]
)
runai_cc_test(
    name = "client_mgr_test",
    srcs = ["client_mgr_test.cc"],
    deps = [
        ":client_mgr",
        "//common/backend_api/object_storage",
        "//common/storage_uri",
        "//common/obj_store_credentials",
        "//common/obj_store_wrapper",
        "//utils/random",
        ],
)
