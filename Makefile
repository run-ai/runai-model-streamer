X86_64_ARCH := x86_64
AARCH64_ARCH := aarch64

.PHONY: build build_aarch64 build_x86_64 test test_iouring install

build_x86_64:
	make -C cpp build ARCH=${X86_64_ARCH} && \
	make -C py build ARCH=${X86_64_ARCH}

build_aarch64:
	make -C cpp build ARCH=${AARCH64_ARCH} && \
	make -C py build ARCH=${AARCH64_ARCH}

build: 
	make -C py clean && \
	make build_x86_64 && \
	make build_aarch64

install: build
	make -C py install

test: install
	make -C cpp test && \
	make -C py test && \
	make -C tests all

# The manual io_uring sweep. NOT run by CI, which has no io_uring, and not part of `test`.
#
# Run it inside the dev container: devcontainer.json sets seccomp=unconfined, and without that
# io_uring_setup returns EPERM even when the kernel allows io_uring. On a host or a default container
# this target fails, which is the point - it is meant to fail where the ring is missing.
#
# What each part adds:
#
#   cpp test_iouring    the same 53 targets, but the io_uring tests must RUN. They skip wherever the
#                       ring is missing, and a silent skip looks exactly like a pass. This includes
#                       the only tests that check our O_DIRECT alignment rule against the KERNEL
#                       rather than against MockIoEngine.
#
#   test-unit-real x3   the Python tests against the real library, once per reader. `make test` runs
#                       them against the mock, which stubs the C API and never reaches a reader at
#                       all, so it cannot tell the three apart.
#
# Each strategy list keeps sync_buffered last, as the fallback. Never repeat a name: a duplicate is
# rejected by the parser and every test that builds a streamer then fails together.
test_iouring: install
	make -C cpp test_iouring && \
	make -C py/runai_model_streamer test-unit-real FS_STRATEGY=sync_buffered && \
	make -C py/runai_model_streamer test-unit-real FS_STRATEGY=io_uring_buffered,sync_buffered && \
	make -C py/runai_model_streamer test-unit-real FS_STRATEGY=io_uring_direct,sync_buffered

