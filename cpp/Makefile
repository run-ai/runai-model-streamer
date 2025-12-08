.PHONY: build build_mock test clean

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

build_mock:
	bazel build mock:libstreamer-mock.so

test:
	bazel test //...:all
