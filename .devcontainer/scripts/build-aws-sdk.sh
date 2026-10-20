#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

git clone --recurse-submodules --depth 1 --branch "1.11.584" https://github.com/aws/aws-sdk-cpp /opt/aws-sdk-cpp
cd /opt/aws-sdk-cpp

cmake -DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/x86_64-linux-gnu.cmake \
      -DBUILD_ONLY="s3-crt" \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
      -DCMAKE_PREFIX_PATH="/opt/x86_64-ssl;/opt/x86_64-curl" \
      -DCMAKE_INSTALL_PREFIX="/opt/x86_64-aws" \
      -DFORCE_SHARED_CRT=OFF \
      -DBUILD_SHARED_LIBS=OFF \
      -DENABLE_TESTING=OFF \
      -DAUTORUN_UNIT_TESTS=OFF \
      -DZLIB_LIBRARY="/opt/x86_64-zlib/lib/libz.a" \
      -DZLIB_INCLUDE_DIR="/opt/x86_64-zlib/include" \
      -Dcrypto_LIBRARY="/opt/x86_64-ssl/lib/libcrypto.a" \
      -Dcrypto_INCLUDE_DIR="/opt/x86_64-ssl/include" \
      -DCURL_LIBRARY="/opt/x86_64-curl/lib/libcurl.a" \
      -DCURL_INCLUDE_DIR="/opt/x86_64-curl/include" \
      -DOPENSSL_CRYPTO_LIBRARY="/opt/x86_64-ssl/lib/libssl.a" \
      -DOPENSSL_INCLUDE_DIR="/opt/x86_64-ssl/include"
make -j4
make install

make clean
rm -rf CMakeCache.txt

cmake -DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/aarch64-linux-gnu.cmake \
      -DBUILD_ONLY="s3-crt" \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
      -DCMAKE_PREFIX_PATH="/opt/aarch64-ssl;/opt/aarch64-curl" \
      -DCMAKE_INSTALL_PREFIX="/opt/aarch64-aws" \
      -DFORCE_SHARED_CRT=OFF \
      -DBUILD_SHARED_LIBS=OFF \
      -DENABLE_TESTING=OFF \
      -DAUTORUN_UNIT_TESTS=OFF \
      -DZLIB_LIBRARY="/opt/aarch64-zlib/lib/libz.a" \
      -DZLIB_INCLUDE_DIR="/opt/aarch64-zlib/include" \
      -Dcrypto_LIBRARY="/opt/aarch64-ssl/lib/libcrypto.a" \
      -Dcrypto_INCLUDE_DIR="/opt/aarch64-ssl/include" \
      -DCURL_LIBRARY="/opt/aarch64-curl/lib/libcurl.a" \
      -DCURL_INCLUDE_DIR="/opt/aarch64-curl/include" \
      -DOPENSSL_CRYPTO_LIBRARY="/opt/aarch64-ssl/lib/libssl.a" \
      -DOPENSSL_INCLUDE_DIR="/opt/aarch64-ssl/include"
make -j4
make install
rm -rf /opt/aws-sdk-cpp
