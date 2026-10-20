# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

FROM nvidia/cuda:12.8.0-devel-ubuntu20.04@sha256:cd0f8e1c41628ff2513e9f7c42dd9bd8740fe87fd6615b9c4a6f392995a589c0 AS cuda-headers

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
    gcc-x86-64-linux-gnu g++-x86-64-linux-gnu binutils-x86-64-linux-gnu \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu binutils-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/aarch64-linux-gnu-ld.gold /usr/bin/ld.gold

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
# libtsan_preinit.o, in case a later base image ships one.
RUN printf '%s\n' \
        'extern void __tsan_init(void);' \
        'static void __local_tsan_preinit(void) { __tsan_init(); }' \
        '__attribute__((section(".preinit_array"), used))' \
        'static void (*tsan_preinit)(void) = __local_tsan_preinit;' \
        > /tmp/tsan_preinit.c \
    && TSAN_DIR="$(dirname "$(x86_64-linux-gnu-gcc -print-file-name=libtsan.so)")" \
    && test -f "$TSAN_DIR/libtsan.so" \
    && { test -f "$TSAN_DIR/libtsan_preinit.o" \
         || x86_64-linux-gnu-gcc -c -fPIC /tmp/tsan_preinit.c -o "$TSAN_DIR/libtsan_preinit.o"; } \
    && rm /tmp/tsan_preinit.c

RUN pip install \
    boto3 \
    cpplint==1.5.4 \
    pytest

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    cmake \
    git \
    curl \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

# weed (SeaweedFS), the S3-compatible object store used for local S3 tests. Checksummed like
# liburing/libaio below.
RUN WEED_ARCH=$([ "$(uname -m)" = "aarch64" ] && echo "arm64" || echo "amd64") \
    && if [ "$WEED_ARCH" = "amd64" ]; then WEED_SHA256=31fb804858885f9e7f18b6d3b1da09e824baac3e6a55b5a62c3c4c77e6ed6d7d; \
       else WEED_SHA256=ba5c9def9ff98f78becdf309a50a9f906922f6383a7c4c772d01ede3cc012e16; fi \
    && curl -fsSL -o weed.tar.gz https://github.com/seaweedfs/seaweedfs/releases/download/4.47/linux_${WEED_ARCH}.tar.gz \
    && echo "${WEED_SHA256}  weed.tar.gz" | sha256sum -c - \
    && tar zxf weed.tar.gz \
    && chmod +x weed \
    && mv weed /usr/local/bin/weed \
    && rm -f weed.tar.gz

# CUDA headers only, for compile-time type checking of the driver API. No runtime and no
# libraries: libcuda.so.1 is loaded with dlopen, so the build has no link-time CUDA dependency
# and the image needs no driver.
COPY --from=cuda-headers /usr/local/cuda/include/ /usr/local/cuda/include/

RUN curl -fsSL -o fake-gcs-server_1.52.2.tar.gz https://github.com/fsouza/fake-gcs-server/releases/download/v1.52.2/fake-gcs-server_1.52.2_Linux_$([ "$(uname -m)" = "aarch64" ] && echo "arm64" || echo "amd64").tar.gz \
    && mkdir -p fake-gcs-server \
    && tar zxf fake-gcs-server_1.52.2.tar.gz -C fake-gcs-server \
    && chmod +x fake-gcs-server/fake-gcs-server \
    && mv fake-gcs-server/fake-gcs-server /usr/local/bin/fake-gcs-server \
    && rm -rf fake-gcs-server

# Install Node.js and Azurite
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && npm install -g azurite \
    && rm -rf /var/lib/apt/lists/*

# Build all shared SDK prerequisites before the cloud SDKs.
COPY scripts/build-basic-libs.sh /tmp/build-basic-libs.sh
RUN bash /tmp/build-basic-libs.sh amd64 arm64

ADD aarch64-linux-gnu.cmake /opt/toolchains/aarch64-linux-gnu.cmake
ADD x86_64-linux-gnu.cmake /opt/toolchains/x86_64-linux-gnu.cmake

# Keep AWS in the final image's layer chain so CI can reuse its build layer
# through the published image cache when later dependencies change.
COPY scripts/build-aws-sdk.sh /tmp/build-aws-sdk.sh
RUN bash /tmp/build-aws-sdk.sh

# Build Azure SDK for C++ (optimized with shared CMake options)
SHELL ["/bin/bash", "-c"]
RUN git clone --recurse-submodules https://github.com/Azure/azure-sdk-for-cpp /opt/azure-sdk-cpp && \
    cd /opt/azure-sdk-cpp && \
    git checkout azure-storage-blobs_12.15.0 && \
    \
    # Helper function to build Azure SDK components
    build_azure_sdk_components() { \
        local arch=$1 prefix=$2 source=$3 extra_opts=$4; \
        # CMAKE_BUILD_TYPE is REQUIRED, not a nicety. With it unset cmake adds no optimisation flag at
        # all and gcc falls back to its own default of -O0. Measured before this line: every function
        # in libazure-core.a and libazure-storage-blobs.a opened with a frame-pointer prologue and
        # spilled its arguments to the stack, which is what -O0 does and -O1 upwards does not.
        #
        # aws-sdk-cpp gets away with the same omission because it sets a default build type in its own
        # CMakeLists. azure-sdk-for-cpp does not, so it has to be stated here.
        local common_opts="-DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/${arch}-linux-gnu.cmake \
            -DCMAKE_BUILD_TYPE=Release \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
            -DCMAKE_INSTALL_PREFIX=${prefix} \
            -DBUILD_SHARED_LIBS=OFF \
            -DBUILD_TESTING=OFF \
            -DCURL_LIBRARY=${prefix%azure}curl/lib/libcurl.a \
            -DCURL_INCLUDE_DIR=${prefix%azure}curl/include \
            -DOPENSSL_ROOT_DIR=${prefix%azure}ssl \
            -DOPENSSL_INCLUDE_DIR=${prefix%azure}ssl/include \
            -DOPENSSL_CRYPTO_LIBRARY=${prefix%azure}ssl/lib/libcrypto.a \
            -DOPENSSL_SSL_LIBRARY=${prefix%azure}ssl/lib/libssl.a \
            -DLIBXML2_LIBRARY=${prefix%azure}xml2/lib/libxml2.a \
            -DLIBXML2_INCLUDE_DIR=${prefix%azure}xml2/include/libxml2"; \
        cmake -B build-${arch}-${source##*/} -S ${source} \
            ${common_opts} \
            -DCMAKE_PREFIX_PATH="${prefix%azure}ssl;${prefix%azure}curl;${prefix%azure}xml2${extra_opts:+;}${extra_opts}" \
            ${@:5} && \
        cmake --build build-${arch}-${source##*/} -j4 && \
        cmake --install build-${arch}-${source##*/}; \
    } && \
    \
    # x86_64 builds
    build_azure_sdk_components x86_64 /opt/x86_64-azure sdk/core/azure-core "" -DBUILD_TRANSPORT_CURL=ON -DDISABLE_AZURE_CORE_OPENTELEMETRY=ON && \
    build_azure_sdk_components x86_64 /opt/x86_64-azure sdk/storage/azure-storage-common /opt/x86_64-azure && \
    build_azure_sdk_components x86_64 /opt/x86_64-azure sdk/storage/azure-storage-blobs /opt/x86_64-azure && \
    build_azure_sdk_components x86_64 /opt/x86_64-azure sdk/identity/azure-identity /opt/x86_64-azure && \
    \
    # aarch64 builds
    build_azure_sdk_components aarch64 /opt/aarch64-azure sdk/core/azure-core "" -DBUILD_TRANSPORT_CURL=ON -DDISABLE_AZURE_CORE_OPENTELEMETRY=ON && \
    build_azure_sdk_components aarch64 /opt/aarch64-azure sdk/storage/azure-storage-common /opt/aarch64-azure && \
    build_azure_sdk_components aarch64 /opt/aarch64-azure sdk/storage/azure-storage-blobs /opt/aarch64-azure && \
    build_azure_sdk_components aarch64 /opt/aarch64-azure sdk/identity/azure-identity /opt/aarch64-azure && \
    \
    rm -rf /opt/azure-sdk-cpp

# Create symlinks for native architecture Azure SDK
RUN NATIVE_ARCH=$(uname -m | sed 's/x86_64/x86_64/;s/aarch64/aarch64/') && \
    if [ -d /opt/${NATIVE_ARCH}-azure ]; then \
        ln -sf /opt/${NATIVE_ARCH}-azure/include/azure /usr/local/include/azure && \
        ln -sf /opt/${NATIVE_ARCH}-azure/lib/* /usr/local/lib/; \
    fi

# Keep I/O libraries last so their changes can reuse the cloud SDK build layers.
COPY scripts/build-io-libs.sh /tmp/build-io-libs.sh
RUN bash /tmp/build-io-libs.sh amd64 arm64
