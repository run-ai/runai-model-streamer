.PHONY: build build_mock build_azure_test test test_iouring clean

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

# Same suite, but io_uring tests must RUN rather than skip.
#
# They skip wherever the ring is unavailable - CI, and any host whose kernel or seccomp profile
# blocks it - so on a host that is supposed to have io_uring a silent skip looks exactly like a pass.
# This target says "io_uring works here", turning that skip into a failure.
#
# Opt-in, and deliberately NOT in devcontainer.json: that file is shared with CI, which has no
# io_uring, and with contributors whose hosts may not either.
#
# --test_env, not a plain export: bazel does not forward the environment into test actions.
test_iouring:
	bazel test //...:all --test_env=RUNAI_STREAMER_REQUIRE_IO_URING=1
