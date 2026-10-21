# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM ubuntu:20.04 AS base

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
    gcc-x86-64-linux-gnu g++-x86-64-linux-gnu binutils-x86-64-linux-gnu \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu binutils-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/aarch64-linux-gnu-ld.gold /usr/bin/ld.gold

ARG TARGETARCH
# Preserve the dual-architecture image by default; native candidates pass one architecture.
ARG DEPENDENCY_ARCHES="amd64 arm64"

# ThreadSanitizer (`bazel test --config=tsan`).
#
# Ubuntu 20.04 ships libtsan.so for gcc-9 but NOT libtsan_preinit.o, so -fsanitize=thread fails to
# link. This is that file, which is all of GCC's own libsanitizer/tsan/tsan_preinit.c: it registers
# __tsan_init in .preinit_array so the runtime starts before any constructor runs.
#
# Built with the CROSS compiler that will consume it, so the object matches its target whatever the
# host architecture is. x86_64 only - there is no libtsan for the aarch64 cross toolchain.
#
# The `test -f libtsan.so` is not a formality: -print-file-name echoes its argument back when the
# file is not there, so without it the object would land in the build directory and --config=tsan
# would keep failing with the same confusing error. The second test never shadows a real
# libtsan_preinit.o, in case a later base image ships one. The ARM64-only candidate does not
# consume this x86_64 object; retain it for the legacy and AMD64 images.
RUN if [ "$DEPENDENCY_ARCHES" != arm64 ]; then \
    printf '%s\n' \
        'extern void __tsan_init(void);' \
        'static void __local_tsan_preinit(void) { __tsan_init(); }' \
        '__attribute__((section(".preinit_array"), used))' \
        'static void (*tsan_preinit)(void) = __local_tsan_preinit;' \
        > /tmp/tsan_preinit.c \
    && TSAN_DIR="$(dirname "$(x86_64-linux-gnu-gcc -print-file-name=libtsan.so)")" \
    && test -f "$TSAN_DIR/libtsan.so" \
    && { test -f "$TSAN_DIR/libtsan_preinit.o" \
         || x86_64-linux-gnu-gcc -c -fPIC /tmp/tsan_preinit.c -o "$TSAN_DIR/libtsan_preinit.o"; } \
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
COPY .devcontainer/scripts/install-test-services.sh /tmp/install-test-services.sh
RUN bash /tmp/install-test-services.sh "$TARGETARCH"

# CUDA headers only, for compile-time type checking of the driver API. No runtime and no
# libraries: libcuda.so.1 is loaded with dlopen, so the build has no link-time CUDA dependency
# and the image needs no driver.
COPY .devcontainer/scripts/install-cuda-headers.sh /tmp/install-cuda-headers.sh
RUN bash /tmp/install-cuda-headers.sh "$TARGETARCH"

# Build all shared SDK prerequisites before the cloud SDKs.
# DEPENDENCY_ARCHES is deliberately split into the scripts' architecture arguments.
COPY .devcontainer/scripts/build-basic-libs.sh /tmp/build-basic-libs.sh
RUN bash /tmp/build-basic-libs.sh $DEPENDENCY_ARCHES

COPY .devcontainer/aarch64-linux-gnu.cmake /opt/toolchains/aarch64-linux-gnu.cmake
COPY .devcontainer/x86_64-linux-gnu.cmake /opt/toolchains/x86_64-linux-gnu.cmake

# Keep AWS in the final image's layer chain so CI can reuse its build layer
# through the published image cache when later dependencies change.
COPY .devcontainer/scripts/build-aws-sdk.sh /tmp/build-aws-sdk.sh
RUN bash /tmp/build-aws-sdk.sh $DEPENDENCY_ARCHES

# Build Azure SDK for C++ (optimized with shared CMake options)
SHELL ["/bin/bash", "-c"]
COPY .devcontainer/scripts/build-azure-sdk.sh /tmp/build-azure-sdk.sh
RUN bash /tmp/build-azure-sdk.sh --native-arch "$TARGETARCH" $DEPENDENCY_ARCHES

# Compile the pinned Google SDK once per image architecture. Only the installed
# archives and headers enter the final image, not sources or the Bazel cache.
FROM base AS gcs-builder
COPY cpp/.bazelversion cpp/.bazelrc /build/gcs/
RUN BAZEL_VERSION=$(cat /build/gcs/.bazelversion) && \
    case "$(uname -m)" in \
        x86_64) BAZEL_SHA256=ac6249d1192aea9feaf49dfee2ab50c38cee2454b00cf29bbec985a11795c025 ;; \
        aarch64) BAZEL_SHA256=2d86d36db0c9af15747ff02a80e6db11a45d68f868ea8f62f489505c474f0099 ;; \
        *) exit 1 ;; \
    esac && \
    curl -fsSL -o /usr/local/bin/bazel \
        "https://github.com/bazelbuild/bazel/releases/download/${BAZEL_VERSION}/bazel-${BAZEL_VERSION}-linux-$(uname -m | sed 's/aarch64/arm64/')" && \
    echo "${BAZEL_SHA256}  /usr/local/bin/bazel" | sha256sum -c - && \
    chmod +x /usr/local/bin/bazel
COPY cpp/toolchain /build/gcs/toolchain/
COPY cpp/third_party/gcp_repo.bzl cpp/third_party/BUILD /build/gcs/third_party/
COPY .devcontainer/gcs/WORKSPACE .devcontainer/gcs/BUILD.bazel \
    .devcontainer/gcs/export.bzl .devcontainer/gcs/install.py \
    .devcontainer/gcs/build.sh .devcontainer/gcs/test_install.py /build/gcs/
RUN --mount=type=cache,target=/root/.cache/bazel,sharing=locked \
    python3 -B -m unittest discover -s /build/gcs -p test_install.py && \
    bash /build/gcs/build.sh $DEPENDENCY_ARCHES

FROM base AS devcontainer
COPY --from=gcs-builder /opt/gcs-sdk /opt/gcs-sdk

# Keep I/O libraries last so their changes can reuse the cloud SDK build layers.
COPY .devcontainer/scripts/build-io-libs.sh /tmp/build-io-libs.sh
RUN bash /tmp/build-io-libs.sh $DEPENDENCY_ARCHES
