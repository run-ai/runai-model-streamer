#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Build the complete library group for each requested Docker architecture.
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
            echo "Unsupported library architecture: $target_arch" >&2
            exit 2
            ;;
    esac
done

build_openssl() {
    local arch

    curl -fsSL -o openssl-3.5.0.tar.gz https://www.openssl.org/source/openssl-3.5.0.tar.gz
    tar zxf openssl-3.5.0.tar.gz
    cd openssl-3.5.0

    for arch in "${build_arches[@]}"; do
        case "$arch" in
            x86_64)
                ./Configure no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --cross-compile-prefix=/usr/bin/x86_64-linux-gnu- --prefix=/opt/x86_64-ssl --openssldir=/opt/x86_64-ssl --libdir=lib linux-x86_64
                make -j4 > /dev/null
                make install_sw > /dev/null
                make clean > /dev/null
                make distclean > /dev/null
                ;;
            aarch64)
                ./Configure no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --libdir=lib --cross-compile-prefix=/usr/bin/aarch64-linux-gnu- --prefix=/opt/aarch64-ssl --openssldir=/opt/aarch64-ssl linux-aarch64
                make -j4 > /dev/null
                make install_sw > /dev/null
                make clean > /dev/null
                make distclean > /dev/null
                ;;
        esac
    done

    cd -
    rm -rf openssl-3.5.0 openssl-3.5.0.tar.gz
}

build_curl() {
    local arch

    curl -fsSL -o curl-8.13.0.tar.gz https://github.com/curl/curl/releases/download/curl-8_13_0/curl-8.13.0.tar.gz
    tar zxf curl-8.13.0.tar.gz
    cd curl-8.13.0

    for arch in "${build_arches[@]}"; do
        case "$arch" in
            x86_64)
                AR=/usr/bin/x86_64-linux-gnu-ar \
                AS=/usr/bin/x86_64-linux-gnu-as \
                LD=/usr/bin/x86_64-linux-gnu-ld \
                RANLIB=/usr/bin/x86_64-linux-gnu-ranlib \
                CC=/usr/bin/x86_64-linux-gnu-gcc \
                NM=/usr/bin/x86_64-linux-gnu-nm \
                CPPFLAGS=-I/opt/x86_64-ssl/include \
                LDFLAGS=-L/opt/x86_64-ssl/lib \
                CFLAGS="-fPIC" \
                ./configure --host=$(gcc -print-multiarch) --build=x86_64-linux-gnu --disable-shared --without-libpsl --with-openssl=/opt/x86_64-ssl --prefix=/opt/x86_64-curl
                make VERBOSE=1 -j4 > /dev/null
                make install > /dev/null
                make clean > /dev/null
                ;;
            aarch64)
                AR=/usr/bin/aarch64-linux-gnu-ar \
                AS=/usr/bin/aarch64-linux-gnu-as \
                LD=/usr/bin/aarch64-linux-gnu-ld \
                RANLIB=/usr/bin/aarch64-linux-gnu-ranlib \
                CC=/usr/bin/aarch64-linux-gnu-gcc \
                NM=/usr/bin/aarch64-linux-gnu-nm \
                CFLAGS="-fPIC" \
                ./configure --host=$(gcc -print-multiarch) --build=aarch64-linux-gnu --disable-shared --without-libpsl --with-openssl=/opt/aarch64-ssl --prefix=/opt/aarch64-curl
                make -j4 > /dev/null
                make install > /dev/null
                make clean > /dev/null
                ;;
        esac
    done

    cd -
    rm -rf curl-7.83.1 curl-7.83.1.tar.gz
}

build_zlib() {
    local arch

    curl -fsSL -o zlib-1.3.1.tar.gz https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz
    tar zxf zlib-1.3.1.tar.gz
    cd zlib-1.3.1

    for arch in "${build_arches[@]}"; do
        case "$arch" in
            x86_64)
                AR=/usr/bin/x86_64-linux-gnu-ar \
                AS=/usr/bin/x86_64-linux-gnu-as \
                LD=/usr/bin/x86_64-linux-gnu-ld \
                RANLIB=/usr/bin/x86_64-linux-gnu-ranlib \
                CC=/usr/bin/x86_64-linux-gnu-gcc \
                NM=/usr/bin/x86_64-linux-gnu-nm \
                ./configure --static --shared --prefix=/opt/x86_64-zlib
                make -j4 > /dev/null
                make install > /dev/null
                make clean > /dev/null
                ;;
            aarch64)
                AR=/usr/bin/aarch64-linux-gnu-ar \
                AS=/usr/bin/aarch64-linux-gnu-as \
                LD=/usr/bin/aarch64-linux-gnu-ld \
                RANLIB=/usr/bin/aarch64-linux-gnu-ranlib \
                CC=/usr/bin/aarch64-linux-gnu-gcc \
                NM=/usr/bin/aarch64-linux-gnu-nm \
                ./configure --static --shared --prefix=/opt/aarch64-zlib
                make -j4 > /dev/null
                make install > /dev/null
                make clean > /dev/null
                ;;
        esac
    done

    cd -
    rm -rf zlib-1.3.1 zlib-1.3.1.tar.gz
}

# -O2 is explicit because setting CFLAGS at all REPLACES autoconf's own default of "-g -O2".
# Passing only -fPIC therefore silently removed the optimisation. curl survives the same treatment
# a few blocks up only because its configure applies --enable-optimize itself; libxml2 does not.
build_libxml2() {
    local arch

    curl -fsSL -o libxml2-2.13.5.tar.xz https://download.gnome.org/sources/libxml2/2.13/libxml2-2.13.5.tar.xz
    tar xf libxml2-2.13.5.tar.xz
    cd libxml2-2.13.5

    for arch in "${build_arches[@]}"; do
        case "$arch" in
            x86_64)
                AR=/usr/bin/x86_64-linux-gnu-ar \
                AS=/usr/bin/x86_64-linux-gnu-as \
                LD=/usr/bin/x86_64-linux-gnu-ld \
                RANLIB=/usr/bin/x86_64-linux-gnu-ranlib \
                CC=/usr/bin/x86_64-linux-gnu-gcc \
                NM=/usr/bin/x86_64-linux-gnu-nm \
                CFLAGS="-fPIC -O2" \
                ./configure --host=x86_64-linux-gnu \
                --disable-shared --enable-static \
                --without-python --without-lzma --without-zlib \
                --prefix=/opt/x86_64-xml2
                make -j4 > /dev/null
                make install > /dev/null
                make distclean > /dev/null
                ;;
            aarch64)
                AR=/usr/bin/aarch64-linux-gnu-ar \
                AS=/usr/bin/aarch64-linux-gnu-as \
                LD=/usr/bin/aarch64-linux-gnu-ld \
                RANLIB=/usr/bin/aarch64-linux-gnu-ranlib \
                CC=/usr/bin/aarch64-linux-gnu-gcc \
                NM=/usr/bin/aarch64-linux-gnu-nm \
                CFLAGS="-fPIC -O2" \
                ./configure --host=aarch64-linux-gnu \
                --disable-shared --enable-static \
                --without-python --without-lzma --without-zlib \
                --prefix=/opt/aarch64-xml2
                make -j4 > /dev/null
                make install > /dev/null
                make distclean > /dev/null
                ;;
        esac
    done

    cd -
    rm -rf libxml2-2.13.5 libxml2-2.13.5.tar.xz
}

build_openssl
build_curl
build_zlib
build_libxml2
