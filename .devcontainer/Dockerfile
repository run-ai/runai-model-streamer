FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    g++ \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
    cpplint==1.5.4

RUN mkdir -p /opt/toolchains && \
    wget "https://releases.linaro.org/components/toolchain/binaries/7.2-2017.11/aarch64-linux-gnu/gcc-linaro-7.2.1-2017.11-x86_64_aarch64-linux-gnu.tar.xz" && \
    tar -xf gcc-linaro-7.2.1-2017.11-x86_64_aarch64-linux-gnu.tar.xz -C /opt/toolchains && \
    mv /opt/toolchains/gcc-linaro-7.2.1-2017.11-x86_64_aarch64-linux-gnu /opt/toolchains/aarch64-linux-gnu && \
    rm -f gcc-linaro-7.2.1-2017.11-x86_64_aarch64-linux-gnu.tar.xz

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

RUN curl -fsSL -o openssl-1.1.1o.tar.gz https://www.openssl.org/source/openssl-1.1.1o.tar.gz && \
    tar zxf openssl-1.1.1o.tar.gz && \
    cd openssl-1.1.1o && \
    ./config no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --prefix=/opt/ssl --openssldir=/opt/ssl && \
    make -j4 > /dev/null && \
    make install_sw > /dev/null && \
    make clean > /dev/null && \
    make distclean > /dev/null && \
    ./Configure no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --cross-compile-prefix=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu- --prefix=/opt/aarch64-ssl --openssldir=/opt/aarch64-ssl linux-aarch64 && \
    make -j4 > /dev/null && \
    make install_sw > /dev/null && \
    make clean > /dev/null && \
    make distclean > /dev/null && \
    rm -rf openssl-1.1.1o openssl-1.1.1o.tar.gz


RUN curl -fsSL -o curl-7.83.1.tar.gz https://github.com/curl/curl/releases/download/curl-7_83_1/curl-7.83.1.tar.gz && \
    tar zxf curl-7.83.1.tar.gz && \
    cd curl-7.83.1 && \
    ./configure --disable-shared --with-openssl=/opt/ssl --prefix=/opt/curl && \
    make -j4 > /dev/null && \
    make install > /dev/null && \
    make clean > /dev/null  && \
    AR=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ar \
    AS=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-as \
    LD=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ld \
    RANLIB=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ranlib \
    CC=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-gcc \
    NM=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-nm \
    ./configure --host=x86_64-linux-gnu --build=aarch64-linux-gnu --disable-shared --with-openssl=/opt/aarch64-ssl --prefix=/opt/aarch64-curl && \
    make -j4 > /dev/null && \
    make install > /dev/null && \
    make clean > /dev/null && \
    rm -rf curl-7.83.1 curl-7.83.1.tar.gz

RUN curl -fsSL -o zlib-1.3.1.tar.gz https://github.com/madler/zlib/releases/download/v1.3.1/zlib-1.3.1.tar.gz && \
    tar zxf zlib-1.3.1.tar.gz && \
    cd zlib-1.3.1 && \
    ./configure --static --prefix=/opt/zlib && \
    make -j4 > /dev/null && \
    make install > /dev/null && \
    make clean > /dev/null  && \
    AR=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ar \
    AS=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-as \
    LD=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ld \
    RANLIB=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-ranlib \
    CC=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-gcc \
    NM=/opt/toolchains/aarch64-none-linux-gnu/bin/aarch64-none-linux-gnu-nm \
    ./configure --static --prefix=/opt/aarch64-zlib && \
    make -j4 > /dev/null && \
    make install > /dev/null && \
    make clean > /dev/null && \
    rm -rf zlib-1.3.1 zlib-1.3.1.tar.gz

RUN git clone --recurse-submodules --depth 1 --branch "1.11.321" https://github.com/aws/aws-sdk-cpp /opt/aws-sdk-cpp && \
    cd /opt/aws-sdk-cpp && \
    cmake -DBUILD_ONLY="s3-crt" \
          -DCMAKE_PREFIX_PATH="/opt/zlib;/opt/ssl;/opt/curl" \
          -DCMAKE_INSTALL_PREFIX="/opt/aws" \
          -DFORCE_SHARED_CRT=OFF \
          -DBUILD_SHARED_LIBS=OFF \
          -DENABLE_TESTING=OFF \
          -DAUTORUN_UNIT_TESTS=OFF && \
    make -j4 && \
    make install && \
    rm -rf /opt/aws-sdk-cpp

ADD aarch64-none-linux-gnu.cmake /opt/toolchains/aarch64-none-linux-gnu.cmake
RUN git clone --recurse-submodules --depth 1 --branch "1.11.321" https://github.com/aws/aws-sdk-cpp /opt/aws-sdk-cpp && \
    cd /opt/aws-sdk-cpp && \
    cmake -DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/aarch64-none-linux-gnu.cmake \
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
