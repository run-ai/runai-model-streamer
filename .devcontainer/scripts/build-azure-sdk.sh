#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# The image architecture selects the SDK exposed through /usr/local.
if (( $# < 3 )) || [[ "$1" != --native-arch ]]; then
    echo "Usage: $0 --native-arch <amd64|arm64> <amd64|arm64> [amd64|arm64 ...]" >&2
    exit 2
fi
case "$2" in
    amd64) native_arch=x86_64 ;;
    arm64) native_arch=aarch64 ;;
    *)
        echo "Unsupported native architecture: $2" >&2
        exit 2
        ;;
esac
shift 2

# Accept Docker architecture names; paths and toolchains use GNU names.
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

git clone --recurse-submodules https://github.com/Azure/azure-sdk-for-cpp /opt/azure-sdk-cpp
cd /opt/azure-sdk-cpp
git checkout azure-storage-blobs_12.15.0

# Helper function to build Azure SDK components
build_azure_sdk_components() {
    local arch=$1 prefix=$2 source=$3 extra_opts=$4
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
        -DLIBXML2_INCLUDE_DIR=${prefix%azure}xml2/include/libxml2"
    cmake -B build-${arch}-${source##*/} -S ${source} \
        ${common_opts} \
        -DCMAKE_PREFIX_PATH="${prefix%azure}ssl;${prefix%azure}curl;${prefix%azure}xml2${extra_opts:+;}${extra_opts}" \
        ${@:5}
    cmake --build build-${arch}-${source##*/} -j4
    cmake --install build-${arch}-${source##*/}
}

for arch in "${build_arches[@]}"; do
    build_azure_sdk_components "$arch" "/opt/${arch}-azure" sdk/core/azure-core "" -DBUILD_TRANSPORT_CURL=ON -DDISABLE_AZURE_CORE_OPENTELEMETRY=ON
    build_azure_sdk_components "$arch" "/opt/${arch}-azure" sdk/storage/azure-storage-common "/opt/${arch}-azure"
    build_azure_sdk_components "$arch" "/opt/${arch}-azure" sdk/storage/azure-storage-blobs "/opt/${arch}-azure"
    build_azure_sdk_components "$arch" "/opt/${arch}-azure" sdk/identity/azure-identity "/opt/${arch}-azure"
done

rm -rf /opt/azure-sdk-cpp

# Expose the native SDK to tools using the standard include and library paths.
if [ -d /opt/${native_arch}-azure ]; then
    ln -sf /opt/${native_arch}-azure/include/azure /usr/local/include/azure
    ln -sf /opt/${native_arch}-azure/lib/* /usr/local/lib/
fi
