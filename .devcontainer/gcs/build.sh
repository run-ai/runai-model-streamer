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
    amd64) arch=x86_64 ;;
    arm64) arch=aarch64 ;;
    *)
        echo "Unsupported SDK architecture: $1" >&2
        exit 2
        ;;
esac

cd /build/gcs
bazel build //:sdk --config="$arch" --jobs=4
python3 install.py bazel-bin/sdk.json "$(bazel info execution_root)" "/opt/gcs-sdk/$arch" \
    --build-inputs /build/gcs --compiler /usr/bin/g++
bazel shutdown
