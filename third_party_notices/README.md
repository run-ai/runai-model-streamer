# Third-party notices

The native libraries (`libstreamer.so`, `libstreamers3.so`, `libstreamergcs.so`,
`libstreamerazure.so`) statically link third-party code. Apache-2.0 and the other licenses
require us to ship each component's copyright and license text with the binaries. This directory
holds those notices and the tooling to regenerate them.

## Layout

| Path | What it is |
|---|---|
| `components.json` | Manifest, the source of truth: one entry per third-party component (name, version, SPDX license, copyright, linkage, which libraries use it, upstream license/NOTICE file URLs). |
| `licenses/<id>/` | Upstream license and NOTICE files, downloaded verbatim from the pinned version. Committed. |
| `licenses/sources.json` | The URL each downloaded file came from. `fetch` re-downloads a file whose URL in `components.json` changed, and `check` fails if they differ. |
| `generate.py` | `fetch` downloads the files, `build` writes the outputs, `check` fails if the outputs or the downloaded files are stale. |
| `list_linked_components.py` | Reads linker maps and lists what was really linked. |
| `libstreamer.txt`, `libstreamers3.txt`, `libstreamergcs.txt`, `libstreamerazure.txt` | Generated. One notice per native library. |
| `sdk.txt` | Generated. The union of the four, for the C/C++ SDK archive. |

## Where each deliverable gets its notice

| Deliverable | LICENSE | Notice |
|---|---|---|
| Wheel `runai-model-streamer` | `py/runai_model_streamer/LICENSE` | `py/runai_model_streamer/THIRD_PARTY_NOTICES.txt` -> `libstreamer.txt` |
| Wheel `runai-model-streamer-s3` | `py/runai_model_streamer_s3/LICENSE` | `.../THIRD_PARTY_NOTICES.txt` -> `libstreamers3.txt` |
| Wheel `runai-model-streamer-gcs` | `py/runai_model_streamer_gcs/LICENSE` | `.../THIRD_PARTY_NOTICES.txt` -> `libstreamergcs.txt` |
| Wheel `runai-model-streamer-azure` | `py/runai_model_streamer_azure/LICENSE` | `.../THIRD_PARTY_NOTICES.txt` -> `libstreamerazure.txt` |
| C/C++ SDK archive (all four `.so`) | root `LICENSE` | `third_party_notices/sdk.txt`, packaged as `THIRD_PARTY_NOTICES.txt` at the archive root |

The wheel files are symlinks, and each `setup.py` lists `THIRD_PARTY_NOTICES.txt` in
`license_files`, so it lands in `<wheel>.dist-info/`. The archive packaging step
(`sdk/package.py`, where present) must copy `sdk.txt` the same way it copies `LICENSE`.

## Regenerating after a change

Run these whenever a dependency version, a build flag or the set of linked libraries changes.

1. **Use the current devcontainer**: `docker pull ghcr.io/dsx-ai-factory/model-streamer/devcontainer:latest`
   (see `.github/workflows/build-devcontainer.yml`). Prebuilt libraries live in `/opt/<arch>-*`.
2. **Get a linker map per library and architecture**, inside the container, on a copy of the repo:
   ```
   cd cpp
   bazel build streamer:libstreamer.so --config=x86_64 --linkopt=-Wl,-Map=/tmp/libstreamer.map
   bazel build s3:libstreamers3.so --config=x86_64 --linkopt=-Wl,-Map=/tmp/libstreamers3.map
   bazel build gcs:libstreamergcs.so --config=x86_64 --linkopt=-Wl,-Map=/tmp/libstreamergcs.map
   bazel build azure:libstreamerazure.so --config=x86_64 --linkopt=-Wl,-Map=/tmp/libstreamerazure.map
   # then repeat the four builds with --config=aarch64 and different map names
   python3 ../third_party_notices/list_linked_components.py /tmp/*.map
   ```
   The map lists every archive that supplied an object file. Anything linked that is not in
   `components.json` is missing from the notices. The x86_64 and aarch64 sets must be compared;
   they differ slightly (see the notes in `components.json`).
3. **Resolve versions.**
   - `/opt` libraries: the pins in `.devcontainer/Dockerfile`. For aws-sdk-cpp, the `crt/` submodule
     SHAs of the pinned tag give aws-crt-cpp, aws-c-*, aws-checksums and s2n-tls versions
     (`gh api repos/aws/aws-sdk-cpp/contents/crt/aws-crt-cpp?ref=<tag>`, then the same under
     `crt/aws-crt-cpp/crt/`; map each SHA to a tag).
   - Bazel-built libraries (gcs): inside the container run
     `bazel query --output=build //external:<repo>` for each external repository seen in the map.
     In WORKSPACE mode the first definition of a repo name wins, so the resolved version can
     differ from what a dependency's own `.bzl` file says.
4. **Find code the map cannot see.**
   - Header-only code: collect the compiler dependency files
     (`find $(bazel info output_base)/execroot/__main__/bazel-out -name '*.d'`) and list the
     external repositories whose headers were included (nlohmann/json and OpenTelemetry are
     header-only here).
   - Code nested inside a component: read each component's `NOTICE`, `LICENSE` and `third_party/`
     directory (aws-sdk-cpp bundles TinyXml2 and cJSON, BoringSSL bundles fiat-crypto, gRPC bundles
     address_sorting, protobuf bundles upb and utf8_range). Add a component for each one that is
     linked.
   - Optional features: for curl check `curl-config --features` and `libcurl.pc` in
     `/opt/<arch>-curl` (currently zlib and OpenSSL only).
   - The toolchain's static runtime (libstdc++, libgcc, glibc `libc_nonshared.a`): version from
     `dpkg -l | grep -E 'libstdc\+\+|libgcc|libc6-dev'`.
5. **Edit `components.json`**, then:
   ```
   python3 third_party_notices/generate.py fetch    # downloads any new license/NOTICE file
   python3 third_party_notices/generate.py build    # rewrites the generated *.txt
   python3 third_party_notices/generate.py check    # freshness check, also run by the SPDX headers workflow
   ```
   `fetch` reports files that do not exist upstream; remove those entries.
6. **Verify the deliverables**: build the wheels and confirm `dist-info/LICENSE` and
   `dist-info/THIRD_PARTY_NOTICES.txt` are present; inspect the SDK archive for the notice file.

## Rules the generator follows

- Each library gets its own notice, listing only what is linked into that library.
- Upstream license text is reproduced verbatim. An Apache-2.0 text that matches the standard text
  is written once, in Appendix A; any variant (extra sections, third-party licenses appended) is
  reproduced in full.
- An upstream `NOTICE` file is always reproduced (Apache-2.0 section 4(d)).
- Copyright strings in `components.json` are the project's standard attribution; where the license
  text itself carries the copyright (MIT, BSD, zlib, curl) the text is authoritative.
- Dual-licensed components record the option used (liburing: MIT).

## Notes

- The CUDA driver header (`cuda.h`) is compiled against only (`libcuda.so` is loaded with
  `dlopen`); its notice is included because the header requires the disclaimer to be reproduced.
