#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Build for the image architecture supplied by Docker's TARGETARCH.
if (( $# != 1 )); then
    echo "Usage: $0 <amd64|arm64>" >&2
    exit 2
fi
case "$1" in
    amd64)
        arch=x86_64
        bazel_arch=x86_64
        bazel_sha256=ac6249d1192aea9feaf49dfee2ab50c38cee2454b00cf29bbec985a11795c025
        ;;
    arm64)
        arch=aarch64
        bazel_arch=arm64
        bazel_sha256=2d86d36db0c9af15747ff02a80e6db11a45d68f868ea8f62f489505c474f0099
        ;;
    *)
        echo "Unsupported SDK architecture: $1" >&2
        exit 2
        ;;
esac

cd /build/gcs

# Install the SDK's pinned Bazel binary for the native image architecture.
bazel_version=$(cat .bazelversion)
curl -fsSL -o /usr/local/bin/bazel \
    "https://github.com/bazelbuild/bazel/releases/download/${bazel_version}/bazel-${bazel_version}-linux-${bazel_arch}"
echo "${bazel_sha256}  /usr/local/bin/bazel" | sha256sum -c -
chmod +x /usr/local/bin/bazel

python3 -B -m unittest discover -s /build/gcs -p test_install.py

CC=/usr/bin/gcc CXX=/usr/bin/g++ bazel build //:sdk --config="$arch" --jobs=4
python3 install.py bazel-bin/sdk.json "$(bazel info execution_root)" "/opt/gcs-sdk/$arch" \
    --build-inputs /build/gcs --compiler /usr/bin/g++
bazel shutdown
