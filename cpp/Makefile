.PHONY: build build_mock test clean

CROSSTOOL_OPTION := $$( [ -n "${ARCH}" ] && echo "--crosstool_top=@${ARCH}//:toolchain" )
AWS_ARCH_COMPILE_BASE_PATH := $$( [ "${ARCH}" = "aarch64" ] && echo "aarch64-" || echo "" )

build:
	bazel build streamer:libstreamer.so ${CROSSTOOL_OPTION} && \
	bazel build s3:libstreamers3.so \
		--define USE_SYSTEM_LIBS=${USE_SYSTEM_LIBS} \
		--define BASE_PATH=${AWS_ARCH_COMPILE_BASE_PATH} \
		${CROSSTOOL_OPTION}

build_mock:
	bazel build mock:libstreamer-mock.so

test:
	bazel test //...:all --define BASE_PATH=${AWS_ARCH_COMPILE_BASE_PATH}

clean:
	bazel clean
