.PHONY: build build_mock build_azure_test test clean

ARCH := $$(uname -m)

build:
	bazel build streamer:libstreamer.so \
		"--config=${ARCH}" && \
	bazel build s3:libstreamers3.so \
		--define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} \
		"--config=${ARCH}" && \
	bazel build gcs:libstreamergcs.so \
		--define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} \
		"--config=${ARCH}" && \
	bazel build azure:libstreamerazure.so \
		--define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} \
		"--config=${ARCH}"

# Build Azure library with Azurite testing support (enables AZURE_STORAGE_ACCOUNT_KEY)
build_azure_test:
	bazel build azure:libstreamerazure.so \
		--define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} \
		"--config=${ARCH}" \
		--config=azurite

# --config=${ARCH} matches the `build` target above. Without it this lands in a DIFFERENT output tree and
# repoints the bazel-bin symlink there, where libstreamer.so and the backend plugins are stale or absent -
# every later run then fails with `OSError: ... cannot open shared object file`, which reads like a
# product regression and is not one.
build_mock:
	bazel build mock:libstreamer-mock.so \
		"--config=${ARCH}"

test:
	bazel test //...:all
