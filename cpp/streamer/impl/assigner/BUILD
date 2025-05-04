load("//:rules.bzl", "runai_cc_auto_library", "runai_cc_test")

runai_cc_auto_library(
    name = "assigner",
    deps = [
        "//streamer/impl/assigner/file_read_task",
        "//streamer/impl/batches",
        "//common/exception",
        "//common/storage_uri",
        "//streamer/impl/config",
        "//utils/logging"
    ],
)

# runai_cc_test(
#     name = "assigner_test",
#     srcs = ["assigner_test.cc"],
#     deps = [":assigner",
#             "//utils/random",
#             "//utils/temp/file",
#             "//utils/scope_guard",
#     ],
# )
