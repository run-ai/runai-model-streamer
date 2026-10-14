#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Run in a clean consumer container with only the archive, this script, and an S3 endpoint.
archive=$(realpath "${1:?SDK tarball}")
work=$(mktemp -d)
prefix="$work/relocated-sdk"
mkdir -p "$prefix"
cd "$(dirname "$archive")"
tar -xzf "$archive" -C "$prefix" --strip-components=1
# No LD_LIBRARY_PATH: the CMake executable must discover plugins using the library's ORIGIN.
unset LD_LIBRARY_PATH
export PKG_CONFIG_PATH="$prefix/lib/pkgconfig"
test "$(pkg-config --modversion runai-streamer)" = "$(cat "$prefix/VERSION")"
export RUNAI_STREAMER_LOG_TO_STDERR=1
# Copy the example out of the prefix so no executable-relative library search can hide a bug.
cp -R "$prefix/examples/hello_streamer" "$work/example"
cmake -S "$work/example" -B "$work/build" -DCMAKE_PREFIX_PATH="$prefix"
cmake --build "$work/build"
printf 'hello from the standalone SDK\n' > "$work/input"
size=$(wc -c < "$work/input")
check_reads() {
    "$1" "$work/input" "$size" > "$work/output"
    cmp "$work/input" "$work/output"
    "$1" "${SDK_S3_URI:?S3 fixture URI}" "$size" > "$work/output"
    cmp "$work/input" "$work/output"
}
# Before ldconfig, only the core library's ORIGIN can find the S3 plugin.
check_reads "$work/build/hello_streamer"
# Register the non-default prefix for plain pkg-config consumers, as a user would.
printf '%s\n' "$prefix/lib" > /etc/ld.so.conf.d/runai-streamer-sdk.conf
ldconfig
# Word splitting is intentional for pkg-config's compiler and linker flags.
# The documented pkg-config command uses a prefix without spaces.
# shellcheck disable=SC2046
cc -std=c99 -Wall -Wextra -Werror "$work/example/hello_streamer.c" $(pkg-config --cflags --libs runai-streamer) -o "$work/hello-c"
# shellcheck disable=SC2046
c++ -x c++ -std=c++17 -Wall -Wextra -Werror "$work/example/hello_streamer.c" $(pkg-config --cflags --libs runai-streamer) -o "$work/hello-cpp"
check_reads "$work/hello-c"
check_reads "$work/hello-cpp"
# Force eager relocation of every plugin to catch undeclared runtime dependencies too.
cat > "$work/plugins.c" <<'C'
#include <dlfcn.h>
#include <stdio.h>
int main(int argc, char **argv) {
    for (int i = 1; i < argc; ++i) {
        void *handle = dlopen(argv[i], RTLD_NOW | RTLD_LOCAL);
        if (!handle) { fprintf(stderr, "%s\n", dlerror()); return 1; }
        dlclose(handle);
    }
    return 0;
}
C
cc -std=c99 -Wall -Wextra -Werror "$work/plugins.c" -ldl -o "$work/plugins"
"$work/plugins" "$prefix/lib/libstreamers3.so" "$prefix/lib/libstreamergcs.so" "$prefix/lib/libstreamerazure.so"
