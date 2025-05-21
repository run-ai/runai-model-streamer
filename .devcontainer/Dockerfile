FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    gcc-x86-64-linux-gnu g++-x86-64-linux-gnu binutils-x86-64-linux-gnu \
    gcc-aarch64-linux-gnu g++-aarch64-linux-gnu binutils-aarch64-linux-gnu \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/aarch64-linux-gnu-ld.gold /usr/bin/ld.gold

RUN pip install \
    boto3 \
    cpplint==1.5.4

ARG BAZEL=4.2.1
RUN ARCH=$([ "$(uname -m)" = "aarch64" ] && echo "arm64" || uname -m); \
    wget -O /usr/local/bin/bazel "https://github.com/bazelbuild/bazel/releases/download/$BAZEL/bazel-$BAZEL-linux-$ARCH" && \
    chmod +x /usr/local/bin/bazel

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    cmake \
    git \
    curl \
    libssl-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o openssl-3.5.0.tar.gz https://www.openssl.org/source/openssl-3.5.0.tar.gz && \
    tar zxf openssl-3.5.0.tar.gz && \
    cd openssl-3.5.0 && \

    ./Configure no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --cross-compile-prefix=/usr/bin/x86_64-linux-gnu- --prefix=/opt/x86_64-ssl --openssldir=/opt/x86_64-ssl --libdir=lib linux-x86_64 && \
        make -j4 > /dev/null && \
        make install_sw > /dev/null && \
        make clean > /dev/null && \
        make distclean > /dev/null && \

    ./Configure no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --libdir=lib --cross-compile-prefix=/usr/bin/aarch64-linux-gnu- --prefix=/opt/aarch64-ssl --openssldir=/opt/aarch64-ssl linux-aarch64 && \
        make -j4 > /dev/null && \
        make install_sw > /dev/null && \
        make clean > /dev/null && \
        make distclean > /dev/null && \

    cd - && \
    rm -rf openssl-3.5.0 openssl-3.5.0.tar.gz

RUN curl -fsSL -o curl-8.13.0.tar.gz https://github.com/curl/curl/releases/download/curl-8_13_0/curl-8.13.0.tar.gz && \
    tar zxf curl-8.13.0.tar.gz && \
    cd curl-8.13.0 && \

    AR=/usr/bin/x86_64-linux-gnu-ar \
    AS=/usr/bin/x86_64-linux-gnu-as \
    LD=/usr/bin/x86_64-linux-gnu-ld \
    RANLIB=/usr/bin/x86_64-linux-gnu-ranlib \
    CC=/usr/bin/x86_64-linux-gnu-gcc \
    NM=/usr/bin/x86_64-linux-gnu-nm \
    CPPFLAGS=-I/opt/x86_64-ssl/include \
    LDFLAGS=-L/opt/x86_64-ssl/lib \
    ./configure --host=x86_64-linux-gnu --build=x86_64-linux-gnu --disable-shared --without-libpsl --with-openssl=/opt/x86_64-ssl --prefix=/opt/x86_64-curl && \
    make VERBOSE=1 -j4 > /dev/null && \
        make install > /dev/null && \
        make clean > /dev/null  && \

    AR=/usr/bin/aarch64-linux-gnu-ar \
    AS=/usr/bin/aarch64-linux-gnu-as \
    LD=/usr/bin/aarch64-linux-gnu-ld \
    RANLIB=/usr/bin/aarch64-linux-gnu-ranlib \
    CC=/usr/bin/aarch64-linux-gnu-gcc \
    NM=/usr/bin/aarch64-linux-gnu-nm \
    ./configure --host=x86_64-linux-gnu --build=aarch64-linux-gnu --disable-shared --without-libpsl --with-openssl=/opt/aarch64-ssl --prefix=/opt/aarch64-curl && \
        make -j4 > /dev/null && \
        make install > /dev/null && \
        make clean > /dev/null && \

    cd - && \
    rm -rf curl-7.83.1 curl-7.83.1.tar.gz

RUN curl -fsSL -o zlib-1.3.1.tar.gz https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz && \
    tar zxf zlib-1.3.1.tar.gz && \
    cd zlib-1.3.1 && \

    AR=/usr/bin/x86_64-linux-gnu-ar \
    AS=/usr/bin/x86_64-linux-gnu-as \
    LD=/usr/bin/x86_64-linux-gnu-ld \
    RANLIB=/usr/bin/x86_64-linux-gnu-ranlib \
    CC=/usr/bin/x86_64-linux-gnu-gcc \
    NM=/usr/bin/x86_64-linux-gnu-nm \
    ./configure --static --shared --prefix=/opt/x86_64-zlib && \
        make -j4 > /dev/null && \
        make install > /dev/null && \
        make clean > /dev/null && \

    AR=/usr/bin/aarch64-linux-gnu-ar \
    AS=/usr/bin/aarch64-linux-gnu-as \
    LD=/usr/bin/aarch64-linux-gnu-ld \
    RANLIB=/usr/bin/aarch64-linux-gnu-ranlib \
    CC=/usr/bin/aarch64-linux-gnu-gcc \
    NM=/usr/bin/aarch64-linux-gnu-nm \
    ./configure --static --shared --prefix=/opt/aarch64-zlib && \
        make -j4 > /dev/null && \
        make install > /dev/null && \
        make clean > /dev/null && \
    cd - && \
    rm -rf zlib-1.3.1 zlib-1.3.1.tar.gz

ADD aarch64-linux-gnu.cmake /opt/toolchains/aarch64-linux-gnu.cmake
ADD x86_64-linux-gnu.cmake /opt/toolchains/x86_64-linux-gnu.cmake
RUN git clone --recurse-submodules --depth 1 --branch "1.11.572" https://github.com/aws/aws-sdk-cpp /opt/aws-sdk-cpp && \
    cd /opt/aws-sdk-cpp && \
    
    cmake -DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/x86_64-linux-gnu.cmake \
          -DBUILD_ONLY="s3-crt" \
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
          -DOPENSSL_INCLUDE_DIR="/opt/x86_64-ssl/include" && \
    make -j4 && \
    make install && \

    make clean && \
    rm -rf CMakeCache.txt && \
    
    cmake -DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/aarch64-linux-gnu.cmake \
          -DBUILD_ONLY="s3-crt" \
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
          -DOPENSSL_INCLUDE_DIR="/opt/aarch64-ssl/include" && \
    make -j4 && \
    make install && \
    rm -rf /opt/aws-sdk-cpp
