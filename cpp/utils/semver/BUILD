load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "semver",
    deps = [
        "//utils/logging",
    ],
)

runai_cc_test(
    name = "semver_test",
    srcs = ["semver_test.cc"],
    deps = [
        ":semver",
        "//utils/random",
    ],
)
