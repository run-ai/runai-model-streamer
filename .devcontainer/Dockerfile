# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# `make build` runs `setup.py bdist_wheel` directly, with no PEP 517 isolation, so it never
# reads py/*/pyproject.toml's setuptools pin. Keep this in sync with that pin by hand. Exact
# pin, not a floor, to match this file's other dependencies. Newest versions still compatible
# with this image's Python 3.8 - setuptools 76.0.0 needs >=3.9, wheel 0.46.0 needs >=3.9.
# Upgrade Ubuntu's pip before setuptools to avoid legacy version-parsing failures.
# pip 25.1 drops Python 3.8 support.
RUN python3 -m pip install --upgrade "pip==25.0.1" \
    && python3 -m pip install "setuptools==75.3.4" "wheel==0.45.1"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    build-essential binutils \
    && rm -rf /var/lib/apt/lists/*

ARG TARGETARCH

# ThreadSanitizer (`bazel test --config=tsan`).
#
# Ubuntu 20.04 ships libtsan.so for gcc-9 but NOT libtsan_preinit.o, so -fsanitize=thread fails to
# link. This is that file, which is all of GCC's own libsanitizer/tsan/tsan_preinit.c: it registers
# __tsan_init in .preinit_array so the runtime starts before any constructor runs.
#
# The `test -f libtsan.so` is not a formality: -print-file-name echoes its argument back when the
# file is not there, so without it the object would land in the build directory and --config=tsan
# would keep failing with the same confusing error. The second test never shadows a real
# libtsan_preinit.o, in case a later base image ships one. Retain the AMD64 workaround
# in the native AMD64 image.
RUN if [ "$TARGETARCH" = amd64 ]; then \
    printf '%s\n' \
        'extern void __tsan_init(void);' \
        'static void __local_tsan_preinit(void) { __tsan_init(); }' \
        '__attribute__((section(".preinit_array"), used))' \
        'static void (*tsan_preinit)(void) = __local_tsan_preinit;' \
        > /tmp/tsan_preinit.c \
    && TSAN_DIR="$(dirname "$(gcc -print-file-name=libtsan.so)")" \
    && test -f "$TSAN_DIR/libtsan.so" \
    && { test -f "$TSAN_DIR/libtsan_preinit.o" \
         || gcc -c -fPIC /tmp/tsan_preinit.c -o "$TSAN_DIR/libtsan_preinit.o"; } \
    && rm /tmp/tsan_preinit.c; \
    fi

RUN pip install \
    boto3 \
    cpplint==1.5.4 \
    pytest

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    cmake \
    git \
    curl \
    xz-utils \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

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
