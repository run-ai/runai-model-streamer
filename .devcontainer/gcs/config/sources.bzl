# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Authoritative SDK source pins shared by the builder and consumers."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

GCP_VERSION = "2.37.0"
GCP_SHA256 = "10867580483cb338e7d50920c2383698f3572cc6b4c7d072e38d5f43755cbd80"
PROTOBUF_VERSION = "27.0"
PROTOBUF_SHA256 = "da288bf1daa6c04d03a9051781caa52aceb9163586bff9aa6cfb12f69b9395aa"
GCP_SOURCE_CHECKSUMS = {"google_cloud_cpp": GCP_SHA256, "protobuf": PROTOBUF_SHA256}

def load_protobuf_repo():
    """Provide protobuf's rule definitions required by rules_cc, without building it."""
    http_archive(
        name = "com_google_protobuf",
        sha256 = PROTOBUF_SHA256,
        strip_prefix = "protobuf-" + PROTOBUF_VERSION,
        url = "https://github.com/protocolbuffers/protobuf/releases/download/v%s/protobuf-%s.tar.gz" % (PROTOBUF_VERSION, PROTOBUF_VERSION),
    )

def load_gcp_repo():
    """Fetch SDK sources only in the devcontainer's SDK builder workspace."""
    http_archive(
        name = "google_cloud_cpp",
        sha256 = GCP_SHA256,
        strip_prefix = "google-cloud-cpp-" + GCP_VERSION,
        url = "https://github.com/googleapis/google-cloud-cpp/archive/v%s.tar.gz" % GCP_VERSION,
    )
