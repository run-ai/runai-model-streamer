# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG BASE=ubuntu:24.04
FROM ${BASE}
# Eoan is deliberately used only for testing the glibc 2.30 compatibility floor.
RUN if grep -q 'VERSION_ID="19.10"' /etc/os-release; then \
        sed -i -E 's@https?://([a-z.]+\.)?(archive.ubuntu.com|security.ubuntu.com|ports.ubuntu.com)/ubuntu(-ports)?@http://old-releases.ubuntu.com/ubuntu@g' /etc/apt/sources.list; \
    fi && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        gcc g++ cmake make pkg-config binutils ca-certificates libxml2 && \
    ! command -v bazel && ! command -v python3
