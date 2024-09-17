.PHONY: build test install

build:
	make -C py clean
	make -C py build

install: build
	make -C py install

test: install
	make -C py test
	make -C tests all
