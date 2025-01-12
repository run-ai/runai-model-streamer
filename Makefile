.PHONY: build build_aarch64 build_x86_64 test install

build_x86_64:
	make -C cpp clean && \
	make -C cpp build ARCH=x86_64 && \
	make -C py build ARCH=x86_64

build_aarch64:
	make -C cpp clean && \
	make -C cpp build ARCH=aarch64 && \
	make -C py build ARCH=aarch64

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

