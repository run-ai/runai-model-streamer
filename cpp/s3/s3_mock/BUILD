load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamerobj_store.so",
    srcs = ["obj_store_mock.h",
            "obj_store_mock.cc",
    ],
    deps = [
        "//common/backend_api/response",
        "//common/backend_api/object_storage",
        "//common/response_code",
        "//common/obj_store_credentials",
        "//common/range",
        "//utils/env",
        "//utils/random",
        "//utils/logging",
    ],
    ldscript = ":obj_store_mock.ldscript",
    visibility= ["//visibility:public"],
)
