.PHONY: build build_aarch64 build_native test install

build_native:
	make -C cpp clean && \
	make -C cpp build && \
	make -C py build

build_aarch64:
	make -C cpp clean && \
	make -C cpp build ARCH=aarch64 && \
	make -C py build ARCH=aarch64

build: 
	make -C py clean && \
	make build_native && \
	make build_aarch64

install: build
	make -C py install

test: install
	make -C cpp test && \
	make -C py test && \
	make -C tests all

