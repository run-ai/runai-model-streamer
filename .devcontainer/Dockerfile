FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
    g++ \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
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

RUN curl -fsSL -o openssl-1.1.1o.tar.gz https://www.openssl.org/source/openssl-1.1.1o.tar.gz && \
    tar zxf openssl-1.1.1o.tar.gz && \
    cd openssl-1.1.1o && \
    ./config no-ssl2 no-zlib no-shared no-comp no-dynamic-engine enable-ec_nistp_64_gcc_128 --prefix=/opt/ssl --openssldir=/opt/ssl && \
    make -j4 > /dev/null && \
    make install_sw > /dev/null && \
    rm -rf openssl-1.1.1o openssl-1.1.1o.tar.gz

RUN curl -fsSL -o curl-7.83.1.tar.gz https://github.com/curl/curl/releases/download/curl-7_83_1/curl-7.83.1.tar.gz && \
    tar zxf curl-7.83.1.tar.gz && \
    cd curl-7.83.1 && \
    ./configure --disable-shared --with-openssl=/opt/ssl --prefix=/opt/curl && \
    make -j4 > /dev/null && \
    make install > /dev/null && \
    rm -rf curl-7.83.1 curl-7.83.1.tar.gz

RUN git clone --recurse-submodules --depth 1 --branch "1.11.321" https://github.com/aws/aws-sdk-cpp /opt/aws-sdk-cpp && \
    cd /opt/aws-sdk-cpp && \
    cmake -DBUILD_ONLY="s3-crt" \
          -DCMAKE_PREFIX_PATH="/opt/ssl;/opt/curl" \
          -DFORCE_SHARED_CRT=OFF \
          -DBUILD_SHARED_LIBS=OFF \
          -DENABLE_TESTING=OFF \
          -DAUTORUN_UNIT_TESTS=OFF && \
    make && \
    make install
