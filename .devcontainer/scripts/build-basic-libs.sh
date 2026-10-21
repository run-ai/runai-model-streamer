#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# Build for the image architecture supplied by Docker's TARGETARCH.
if (( $# != 1 )); then
    echo "Usage: $0 <amd64|arm64>" >&2
    exit 2
fi
case "$1" in
    amd64) arch=x86_64 ;;
    arm64) arch=aarch64 ;;
    *)
        echo "Unsupported architecture: $1" >&2
        exit 2
        ;;
esac

build_openssl() {
    curl -fsSL -o openssl-3.5.0.tar.gz https://www.openssl.org/source/openssl-3.5.0.tar.gz
    tar zxf openssl-3.5.0.tar.gz
    cd openssl-3.5.0

    ./Configure no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --prefix=/opt/${arch}-ssl --openssldir=/opt/${arch}-ssl --libdir=lib linux-${arch}
    make -j4 > /dev/null
    make install_sw > /dev/null
    make clean > /dev/null
    make distclean > /dev/null

    cd -
    rm -rf openssl-3.5.0 openssl-3.5.0.tar.gz
}

build_curl() {
    curl -fsSL -o curl-8.13.0.tar.gz https://github.com/curl/curl/releases/download/curl-8_13_0/curl-8.13.0.tar.gz
    tar zxf curl-8.13.0.tar.gz
    cd curl-8.13.0

    CPPFLAGS=-I/opt/${arch}-ssl/include \
    LDFLAGS=-L/opt/${arch}-ssl/lib \
    CFLAGS="-fPIC" \
    ./configure --disable-shared --without-libpsl --with-openssl=/opt/${arch}-ssl --prefix=/opt/${arch}-curl
    make VERBOSE=1 -j4 > /dev/null
    make install > /dev/null
    make clean > /dev/null

    cd -
    rm -rf curl-8.13.0 curl-8.13.0.tar.gz
}

build_zlib() {
    curl -fsSL -o zlib-1.3.1.tar.gz https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz
    tar zxf zlib-1.3.1.tar.gz
    cd zlib-1.3.1

    ./configure --static --shared --prefix=/opt/${arch}-zlib
    make -j4 > /dev/null
    make install > /dev/null
    make clean > /dev/null

    cd -
    rm -rf zlib-1.3.1 zlib-1.3.1.tar.gz
}

# -O2 is explicit because setting CFLAGS at all REPLACES autoconf's own default of "-g -O2".
# Passing only -fPIC therefore silently removed the optimisation. curl survives the same treatment
# a few blocks up only because its configure applies --enable-optimize itself; libxml2 does not.
build_libxml2() {
    curl -fsSL -o libxml2-2.13.5.tar.xz https://download.gnome.org/sources/libxml2/2.13/libxml2-2.13.5.tar.xz
    tar xf libxml2-2.13.5.tar.xz
    cd libxml2-2.13.5

    CFLAGS="-fPIC -O2" \
    ./configure \
    --disable-shared --enable-static \
    --without-python --without-lzma --without-zlib \
    --prefix=/opt/${arch}-xml2
    make -j4 > /dev/null
    make install > /dev/null
    make distclean > /dev/null

    cd -
    rm -rf libxml2-2.13.5 libxml2-2.13.5.tar.xz
}

build_openssl
build_curl
build_zlib
build_libxml2
