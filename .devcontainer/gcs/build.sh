#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Accept Docker architecture names; paths and toolchains use GNU names.
if (( $# == 0 )); then
    echo "Usage: $0 <amd64|arm64> [amd64|arm64 ...]" >&2
    exit 2
fi
build_arches=()
for target_arch in "$@"; do
    case "$target_arch" in
        amd64) build_arches+=(x86_64) ;;
        arm64) build_arches+=(aarch64) ;;
        *)
            echo "Unsupported SDK architecture: $target_arch" >&2
            exit 2
            ;;
    esac
done

cd /build/gcs
for arch in "${build_arches[@]}"; do
    bazel build //:sdk --config="$arch" --jobs=4
    python3 install.py bazel-bin/sdk.json "$(bazel info execution_root)" "/opt/gcs-sdk/$arch" \
        --build-inputs /build/gcs --compiler "$arch-linux-gnu-g++"
done
bazel shutdown
