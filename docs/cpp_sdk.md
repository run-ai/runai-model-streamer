# Standalone C and C++ SDK

The SDK provides the same C API used by Python, with no Python, Bazel, or development
container required by your application. It supports Linux x86_64 and aarch64 with
**glibc 2.30 or newer**. It does not support musl, macOS, Windows, or static linking.
Use a C99 or C++17 compiler for the examples. The prebuilt binaries' floor matches
the current `manylinux_2_30` wheels; the older manylinux2014 claim in issue #179
no longer describes these builds.

## Download and install

Release tags are `X.Y.Z`, without a `v` prefix. Choose a published release:

```bash
VERSION=0.17.2
ARCH=$(uname -m)  # x86_64 or aarch64
ASSET=runai-model-streamer-cpp-sdk-${VERSION}-linux-${ARCH}.tar.gz
BASE=https://github.com/dsx-ai-factory/model-streamer/releases/download/${VERSION}
curl -fsSL -O "$BASE/$ASSET"
curl -fsSL -O "$BASE/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS
sudo tar -xzf "$ASSET" -C /usr/local --strip-components=1
sudo ldconfig
```

The release's existing `SHA256SUMS` manifest covers all wheels and SDK archives,
using bare filenames. `--ignore-missing` verifies the assets you downloaded; if
both architectures are present, it verifies both together.

For another prefix, extract with the same `--strip-components=1`. Tell CMake or
pkg-config where it lives. To run applications, register its `lib` directory in
`/etc/ld.so.conf.d/` and run `ldconfig`, or use `LD_LIBRARY_PATH` when installing
without administrator access. CMake also sets a build-tree runtime search path
on the example executable.

The archive contains:

```text
include/streamer/{streamer.h,submission_id.h,response_code.h,device.h}
lib/{libstreamer.so,libstreamers3.so,libstreamergcs.so,libstreamerazure.so}
lib/pkgconfig/runai-streamer.pc
lib/cmake/runai-streamer/runai-streamer-config.cmake
examples/hello_streamer/{hello_streamer.c,CMakeLists.txt}
VERSION
LICENSE
```

`libstreamer.so` serves filesystem reads and loads cloud plugins on demand:
`libstreamers3.so` for S3, `libstreamergcs.so` for GCS, and
`libstreamerazure.so` for Azure Blob Storage. **Keep all four libraries in the
same directory and at the same release version.** The core uses its ELF `$ORIGIN`
search path to find plugins beside itself. Moving a plugin elsewhere causes
object-storage operations to fail with a plugin load error; filesystem reads can
still work. Azure builds linked against shared `libxml2` also require your
distribution's `libxml2` package; that system library is not bundled in the SDK.

## Build and run the example

The example reads `BYTE_COUNT` bytes at offset zero from a filesystem path or an
object-storage URI and writes those bytes to standard output. It checks the
response code and submission identity and drains the streamer before freeing its
buffer, including on failure or timeout.

With CMake:

```bash
PREFIX=/usr/local
cmake -S "$PREFIX/examples/hello_streamer" -B hello-build -DCMAKE_PREFIX_PATH="$PREFIX"
cmake --build hello-build
printf 'hello from the standalone SDK\n' > input
./hello-build/hello_streamer input "$(wc -c < input)" > output
cmp input output
```

Your own CMake project only needs:

```cmake
find_package(runai-streamer REQUIRED)
target_link_libraries(my_app PRIVATE runai::streamer)
```

With pkg-config (this shell command assumes a prefix without spaces):

```bash
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
cc -std=c99 -Wall -Wextra -Werror "$PREFIX/examples/hello_streamer/hello_streamer.c" \
    $(pkg-config --cflags --libs runai-streamer) -o hello_streamer
./hello_streamer input "$(wc -c < input)" > output
cmp input output
```

The same source compiles as C++ with `c++ -x c++ -std=c++17`. No project-specific
preprocessor defines are required. The current public spelling is
`#include <streamer/streamer.h>`, with `runai_file_streamer_*` entry points and
`RunaiFileStreamerSubmissionId` and `RunaiFileStreamerDevice` types.

For S3, provide credentials through the standard AWS environment or credential
files and pass `s3://bucket/key` as the path. For a compatible endpoint:

```bash
export AWS_ENDPOINT_URL=http://localhost:8333
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
export AWS_EC2_METADATA_DISABLED=true RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=0
./hello_streamer s3://sdk-test/input 30 > output
```

Applications can instead call `runai_file_streamer_set_credentials` before their
first object-storage operation. Credentials are scoped to a streamer and set once.

## API rules and configuration

The API is **not thread safe**: callers must not send requests and receive responses
in parallel on the same handle. Each submission owes one response per range,
including a zero-sized range. Keep destination buffers alive until
`runai_file_streamer_end` returns. See the installed `streamer.h` for full lifetime,
error handling, device destination, and concurrency rules.

Return values are the explicit constants in
[`streamer/response_code.h`](../cpp/streamer/api/streamer/response_code.h), from
`RUNAI_FILE_STREAMER_RESPONSE_SUCCESS` (0) through
`RUNAI_FILE_STREAMER_RESPONSE_DEVICE_DRIVER_ERROR` (28). Compare against those
constants rather than numbers; `runai_file_streamer_response_str(code)` supplies a
message. Existing values never change, and new codes are appended. In particular,
`TIMED_OUT` (16) does not mean an outstanding read has finished; teardown must still
drain it. `OBJ_PLUGIN_LOAD_ERROR` (12) means a plugin could not load.

The main settings, also documented in the installed header, are:

| Variable | Meaning / default |
| --- | --- |
| `RUNAI_STREAMER_CONCURRENCY` | Worker count: 16 filesystem, 8 object storage when unset |
| `RUNAI_STREAMER_CHUNK_BYTESIZE` | Read chunk bytes: 2 MiB filesystem; backend default for object storage |
| `RUNAI_STREAMER_FS_STRATEGY` | Ordered candidates; default `io_uring_direct,libaio_direct,sync_buffered` |
| `RUNAI_STREAMER_LOG_TO_STDERR` | Enable diagnostic logging on standard error |

The example explicitly selects `sync_buffered` so an ordinary allocated buffer
works without direct-I/O alignment. Applications can select another strategy with
`runai_file_streamer_set_fs_strategy`. See [environment variables](src/env-vars.md)
for backend-specific settings.

## Compatibility and validation

The ELF ABI node `RUNAI_FILE_STREAMER_1.0` is independent of the package version.
Existing symbols stay in their original node. New symbols require a new node;
incompatible signatures require a new version alongside the old implementation.
Never rename or remove a published node. `sdk/abi.json` pins the current export
map, and CI compares both the linker script and built library against it. A new
node requires an explicit baseline update; do not change an existing node's map.

PR and release CI package the same component artifacts used by the wheels. They
check both architectures' exports, glibc requirements, and `$ORIGIN`, then run
`sdk/test-consumer.sh` on clean Ubuntu 19.10 (glibc 2.30, older than the Ubuntu
20.04 build image) and Ubuntu 24.04 containers on both architectures. The consumer
has system compilers, CMake, pkg-config and libxml2, with no Python or Bazel.
Packaging tests verify `SHA256SUMS`, including downloads containing only SDKs.
The consumer uses the extraction and build commands above, relocates to a non-default
prefix, checks actual filesystem and S3 bytes, and loads every plugin
with eager symbol resolution. The S3 server and provisioning client run separately.
The old distribution is used solely to enforce compatibility, not as a deployment
recommendation. Release publication copies the exact archives that passed these
checks, without rebuilding them.
