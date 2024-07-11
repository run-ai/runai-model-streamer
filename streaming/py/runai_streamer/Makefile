ARCH := $(shell uname -m)

.PHONY: install_dev_dep build mock test clean

install_dev_dep:
	python3 -m pip install -r requirements.dev

build:
	python3 setup.py bdist_wheel --plat-name manylinux1_${ARCH}

mock:
	make -C ../../cpp build_mock

test: mock install_dev_dep
	STREAMER_LIBRARY="../../cpp/bazel-bin/mock/libstreamer-mock.so" python3 -m unittest discover -b runai_streamer

clean:
	rm -rf build/ dist/ runai_streamer.egg-info/
