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

git clone --recurse-submodules --depth 1 --branch "1.11.584" https://github.com/aws/aws-sdk-cpp /opt/aws-sdk-cpp
cd /opt/aws-sdk-cpp

first_build=true
for arch in "${build_arches[@]}"; do
    if [[ "$first_build" == false ]]; then
        make clean
        rm -rf CMakeCache.txt
    fi
    first_build=false

    cmake -DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/${arch}-linux-gnu.cmake \
          -DBUILD_ONLY="s3-crt" \
          -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
          -DCMAKE_PREFIX_PATH="/opt/${arch}-ssl;/opt/${arch}-curl" \
          -DCMAKE_INSTALL_PREFIX="/opt/${arch}-aws" \
          -DFORCE_SHARED_CRT=OFF \
          -DBUILD_SHARED_LIBS=OFF \
          -DENABLE_TESTING=OFF \
          -DAUTORUN_UNIT_TESTS=OFF \
          -DZLIB_LIBRARY="/opt/${arch}-zlib/lib/libz.a" \
          -DZLIB_INCLUDE_DIR="/opt/${arch}-zlib/include" \
          -Dcrypto_LIBRARY="/opt/${arch}-ssl/lib/libcrypto.a" \
          -Dcrypto_INCLUDE_DIR="/opt/${arch}-ssl/include" \
          -DCURL_LIBRARY="/opt/${arch}-curl/lib/libcurl.a" \
          -DCURL_INCLUDE_DIR="/opt/${arch}-curl/include" \
          -DOPENSSL_CRYPTO_LIBRARY="/opt/${arch}-ssl/lib/libcrypto.a" \
          -DOPENSSL_SSL_LIBRARY="/opt/${arch}-ssl/lib/libssl.a" \
          -DOPENSSL_INCLUDE_DIR="/opt/${arch}-ssl/include"
    make -j4
    make install
done
rm -rf /opt/aws-sdk-cpp
