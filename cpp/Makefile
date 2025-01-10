.PHONY: build build_mock test

build:
	bazel build streamer:libstreamer.so $$( [ -n "${ARCH}" ] && echo "--crosstool_top=@${ARCH}//:toolchain" ) && \
	bazel build --define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} s3:libstreamers3.so --define BASE_PATH=$$( [ "${ARCH}" = "aarch64" ] && echo "aarch64-" || echo "" ) $$( [ -n "${ARCH}" ] && echo "--crosstool_top=@${ARCH}//:toolchain" )

build_mock:
	bazel build mock:libstreamer-mock.so

test:
	bazel test //...:all --define BASE_PATH=$$( [ "${ARCH}" = "aarch64" ] && echo "aarch64-" || echo "" )

clean:
	bazel clean
