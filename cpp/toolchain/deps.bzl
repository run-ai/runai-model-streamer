# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Load the Bazel rule dependencies shared with the prebuilt SDK."""

load("@gcs_sdk_config//:deps.bzl", "load_bazel_deps")

def load_toolchain_deps():
    load_bazel_deps()
