.PHONY: build test install

build:
	make -C cpp clean && \
	make -C py clean && \
	make -C cpp build && \
	make -C py build

install: build
	make -C py install

test: install
	make -C cpp test && \
	make -C py test && \
	make -C tests all

