.PHONY: build build_mock test

build:
	bazel build streamer:libstreamer.so && \
	bazel build --define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} s3:libstreamers3.so

build_mock:
	bazel build mock:libstreamer-mock.so

test:
	bazel test //...:all

clean:
	bazel clean
