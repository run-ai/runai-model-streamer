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

RUN wget https://dl.min.io/server/minio/release/linux-$([ "$(uname -m)" = "aarch64" ] && echo "arm64" || echo "amd64")/minio \
    && chmod +x minio \
    && mv minio /usr/local/bin/minio

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
    CFLAGS="-fPIC" \
    ./configure --host=$(gcc -print-multiarch) --build=x86_64-linux-gnu --disable-shared --without-libpsl --with-openssl=/opt/x86_64-ssl --prefix=/opt/x86_64-curl && \
    make VERBOSE=1 -j4 > /dev/null && \
        make install > /dev/null && \
        make clean > /dev/null  && \

    AR=/usr/bin/aarch64-linux-gnu-ar \
    AS=/usr/bin/aarch64-linux-gnu-as \
    LD=/usr/bin/aarch64-linux-gnu-ld \
    RANLIB=/usr/bin/aarch64-linux-gnu-ranlib \
    CC=/usr/bin/aarch64-linux-gnu-gcc \
    NM=/usr/bin/aarch64-linux-gnu-nm \
    CFLAGS="-fPIC" \
    ./configure --host=$(gcc -print-multiarch) --build=aarch64-linux-gnu --disable-shared --without-libpsl --with-openssl=/opt/aarch64-ssl --prefix=/opt/aarch64-curl && \
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
RUN git clone --recurse-submodules --depth 1 --branch "1.11.584" https://github.com/aws/aws-sdk-cpp /opt/aws-sdk-cpp && \
    cd /opt/aws-sdk-cpp && \
    
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
          -DOPENSSL_INCLUDE_DIR="/opt/x86_64-ssl/include" && \
    make -j4 && \
    make install && \

    make clean && \
    rm -rf CMakeCache.txt && \
    
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
          -DOPENSSL_INCLUDE_DIR="/opt/aarch64-ssl/include" && \
    make -j4 && \
    make install && \
    rm -rf /opt/aws-sdk-cpp

RUN curl -fsSL -o libxml2-2.13.5.tar.xz https://download.gnome.org/sources/libxml2/2.13/libxml2-2.13.5.tar.xz && \
    tar xf libxml2-2.13.5.tar.xz && \
    cd libxml2-2.13.5 && \

    AR=/usr/bin/x86_64-linux-gnu-ar \
    AS=/usr/bin/x86_64-linux-gnu-as \
    LD=/usr/bin/x86_64-linux-gnu-ld \
    RANLIB=/usr/bin/x86_64-linux-gnu-ranlib \
    CC=/usr/bin/x86_64-linux-gnu-gcc \
    NM=/usr/bin/x86_64-linux-gnu-nm \
    CFLAGS="-fPIC" \
    ./configure --host=x86_64-linux-gnu \
        --disable-shared --enable-static \
        --without-python --without-lzma --without-zlib \
        --prefix=/opt/x86_64-xml2 && \
        make -j4 > /dev/null && \
        make install > /dev/null && \
        make distclean > /dev/null && \

    AR=/usr/bin/aarch64-linux-gnu-ar \
    AS=/usr/bin/aarch64-linux-gnu-as \
    LD=/usr/bin/aarch64-linux-gnu-ld \
    RANLIB=/usr/bin/aarch64-linux-gnu-ranlib \
    CC=/usr/bin/aarch64-linux-gnu-gcc \
    NM=/usr/bin/aarch64-linux-gnu-nm \
    CFLAGS="-fPIC" \
    ./configure --host=aarch64-linux-gnu \
        --disable-shared --enable-static \
        --without-python --without-lzma --without-zlib \
        --prefix=/opt/aarch64-xml2 && \
        make -j4 > /dev/null && \
        make install > /dev/null && \
        make distclean > /dev/null && \

    cd - && \
    rm -rf libxml2-2.13.5 libxml2-2.13.5.tar.xz

# Build Azure SDK for C++ (optimized with shared CMake options)
SHELL ["/bin/bash", "-c"]
RUN git clone --recurse-submodules https://github.com/Azure/azure-sdk-for-cpp /opt/azure-sdk-cpp && \
    cd /opt/azure-sdk-cpp && \
    git checkout azure-storage-blobs_12.15.0 && \
    \
    # Helper function to build Azure SDK components
    build_azure_sdk_components() { \
        local arch=$1 prefix=$2 source=$3 extra_opts=$4; \
        local common_opts="-DCMAKE_TOOLCHAIN_FILE=/opt/toolchains/${arch}-linux-gnu.cmake \
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

