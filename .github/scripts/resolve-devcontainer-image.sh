#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

if (( $# != 1 )) || [[ "$1" != ghcr.io/*:* || "$1" == *@* ]]; then
    echo "Usage: $0 <ghcr.io/repository/devcontainer:tag>" >&2
    exit 2
fi
image=$1

# Inspect the registry index directly: pulling on the setup runner may resolve only
# that runner's architecture. Both sets of consumers must use the same image pair.
if ! manifest=$(docker buildx imagetools inspect "$image" --format '{{json .Manifest}}'); then
    echo "::error::Cannot access devcontainer $image. Check registry access and publish the matching branch's devcontainer before retrying." >&2
    exit 1
fi
if ! digest=$(jq -er '
    select(.mediaType == "application/vnd.oci.image.index.v1+json" or
           .mediaType == "application/vnd.docker.distribution.manifest.list.v2+json")
    | select([.manifests[] | select(.platform.os == "linux") | .platform.architecture]
             | unique | sort == ["amd64", "arm64"])
    | .digest | strings | select(test("^sha256:[0-9a-f]{64}$"))
' <<< "$manifest"); then
    echo "::error::Devcontainer $image must contain both linux/amd64 and linux/arm64. Run Build and Push DevContainer on the matching branch before retrying." >&2
    exit 1
fi
printf '%s@%s\n' "${image%:*}" "$digest"
