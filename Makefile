X86_64_ARCH := x86_64
AARCH64_ARCH := aarch64

.PHONY: build build_aarch64 build_x86_64 test install

build_x86_64:
	make -C cpp clean && \
	make -C cpp build ARCH=${X86_64_ARCH} && \
	make -C py build ARCH=${X86_64_ARCH}

build_aarch64:
	make -C cpp clean && \
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

