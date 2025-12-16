load("//:rules.bzl", "runai_cc_auto_library",  "runai_cc_test")

runai_cc_auto_library(
    name = "obj_store_wrapper",
    deps = [
        "//common/backend_api/object_storage",
        "//common/storage_uri",
        "//common/obj_store_credentials",
        "//common/backend_api/response",
        "//common/range",
        "//common/exception",
        "//utils/dylib",
        "//utils/env",
        "//utils/semver",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "obj_store_wrapper_test",
    srcs = ["obj_store_wrapper_test.cc"],
    deps = [
        ":obj_store_wrapper",
        "//obj_store/obj_store_mock:libstreamerobj_store.so",
        "//utils/random",
        "//utils/temp/env",
    ],
    data = ["//obj_store/obj_store_mock:libstreamerobj_store.so"],
    linkopts = [
        "-Wl,-rpath,$$ORIGIN/../../obj_store/obj_store_mock",
    ],
)
