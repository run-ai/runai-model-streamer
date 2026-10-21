# Prebuilt Google Cloud C++ SDK

The devcontainer builds the pinned `storage` and `storage_grpc` SDK targets,
including their transitive dependencies, and installs static archives and headers
under `/opt/gcs-sdk/<architecture>`. Normal builds compile our GCS adapter and
tests against that installation. REST and gRPC remain enabled.

Docker's `TARGETARCH` selects the installed SDK (`amd64` or `arm64`). Both CI
and local images build only their native architecture using the native toolchain.
The Bazel repository exposes only installed architectures and reports an
unsupported target clearly.

The SDK uses the application's Bazel version, toolchains, compiler settings and
pinned source definitions. `export.bzl` preserves the transitive `CcInfo` headers,
include paths, defines, static libraries, always-link requirements and linker
flags. `install.py` dereferences generated and virtual headers into a standalone
Bazel package. The final image contains no SDK builder sources or Bazel cache.
Application builds still fetch protobuf's Bazel rule definitions, which
`rules_cc` imports; they do not compile protobuf or its dependencies.

Rebuild the devcontainer after changing SDK versions, toolchains or compiler
settings. The consumer rejects missing SDKs, mismatched SDK/protobuf checksums,
changed Bazel/toolchain inputs and a different compiler version. Both PR image
selection and publication watch the SDK source inputs copied into the image.
Changing `.bazelversion` also requires updating the matching Linux AMD64 and
ARM64 Bazel binary checksums in the Dockerfile.

The first PR adding this SDK builds new candidate images. After merging, allow
**Build and Push DevContainer** to publish the matching image before relying on
it from application-only PRs or release builds. Existing images without the SDK
fail with a rebuild instruction; there is no silent source-build fallback.

Run the installer tests with:

```sh
python3 -B -m unittest discover -s .devcontainer/gcs -p 'test_*.py'
```

Validate the native images with GCS C++ builds/tests and the existing Python
integration suite. Inspect `bazel aquery 'mnemonic("CppCompile", deps(//gcs:libstreamergcs.so))' --config=<architecture>`
to confirm that no Google SDK dependency compilation remains. Measure fresh
application build time separately from SDK image construction and image transfer.
