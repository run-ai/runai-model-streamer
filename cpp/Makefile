.PHONY: build build_mock test clean

ARCH := $$(uname -m)

build:
	bazel build streamer:libstreamer.so \
		"--crosstool_top=@${ARCH}//:toolchain" && \
	bazel build s3:libstreamers3.so \
		--define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} \
		--define BASE_PATH=${ARCH} \
		"--crosstool_top=@${ARCH}//:toolchain"

build_mock:
	bazel build mock:libstreamer-mock.so

test:
	echo bazel test //...:all --define BASE_PATH=${ARCH}

clean:
	bazel clean
