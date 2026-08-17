load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "mount_capabilities",
    deps = [
        "//common/exception",
        "//common/response_code",
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "mount_capabilities_test",
    srcs = ["mount_capabilities_test.cc"],
    deps = [
        ":mount_capabilities",
        "//utils/random",
        "//utils/temp/file",
    ],
)
