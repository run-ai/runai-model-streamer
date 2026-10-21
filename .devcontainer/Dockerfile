# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Multi-architecture index: linux/amd64 and linux/arm64.
FROM mcr.microsoft.com/devcontainers/cpp:3.0.7-ubuntu24.04@sha256:cfd2d7c3f18ab12a760dc8241554c91379ebc2b7dcbaf6bcb46a882fca23722e

USER root

# The base supplies GCC/G++, binutils, CMake, Git, curl, wget, xz and the system
# SSL/curl/XML development packages. Ubuntu's Python is externally managed;
# install project packages in a virtual environment instead of modifying it.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3.12-venv=3.12.3-1ubuntu0.17 \
    && rm -rf /var/lib/apt/lists/* \
    && python3.12 -m venv /opt/model-streamer-venv

ENV VIRTUAL_ENV=/opt/model-streamer-venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

# Keep the existing packaging tool versions during the base-image migration.
# make build invokes setup.py directly, so these are used without PEP 517 isolation.
RUN python3 -m pip install --upgrade "pip==25.0.1" \
    && python3 -m pip install "setuptools==75.3.4" "wheel==0.45.1"

ARG TARGETARCH

# GCC 13 supplies the ThreadSanitizer preinit object on both architectures;
# fail early if a future base changes that instead of synthesizing a replacement.
RUN test -f "$(gcc -print-file-name=libtsan_preinit.o)" \
    && test -f "$(gcc -print-file-name=libtsan.so)"

RUN pip install \
    boto3 \
    cpplint==1.5.4 \
    pytest

# Local storage services used by the integration tests.
COPY scripts/install-test-services.sh /tmp/install-test-services.sh
RUN bash /tmp/install-test-services.sh "$TARGETARCH"

# CUDA headers only, for compile-time type checking of the driver API. No runtime and no
# libraries: libcuda.so.1 is loaded with dlopen, so the build has no link-time CUDA dependency
# and the image needs no driver.
COPY scripts/install-cuda-headers.sh /tmp/install-cuda-headers.sh
RUN bash /tmp/install-cuda-headers.sh "$TARGETARCH"

# Build all shared SDK prerequisites before the cloud SDKs.
COPY scripts/build-basic-libs.sh /tmp/build-basic-libs.sh
RUN bash /tmp/build-basic-libs.sh "$TARGETARCH"

# Keep AWS in the final image's layer chain so CI can reuse its build layer
# through the published image cache when later dependencies change.
COPY scripts/build-aws-sdk.sh /tmp/build-aws-sdk.sh
RUN bash /tmp/build-aws-sdk.sh "$TARGETARCH"

# Build Azure SDK for C++ (optimized with shared CMake options)
SHELL ["/bin/bash", "-c"]
COPY scripts/build-azure-sdk.sh /tmp/build-azure-sdk.sh
RUN bash /tmp/build-azure-sdk.sh "$TARGETARCH"

# Keep I/O libraries last so their changes can reuse the cloud SDK build layers.
COPY scripts/build-io-libs.sh /tmp/build-io-libs.sh
RUN bash /tmp/build-io-libs.sh "$TARGETARCH"
