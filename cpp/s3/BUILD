load("//:rules.bzl", "runai_portable_so")

runai_portable_so(
    name = "libstreamerobj_store.so",
    srcs = ["obj_store.h",
            "obj_store.cc",
    ],
    deps = [
        "//common/exception",
        "//common/range",
        "//common/client_mgr",
        "//utils/semver",
        "//obj_store/client",
        "//obj_store/obj_store_init",
    ],
    ldscript = ":obj_store.ldscript",
)
