ARCH := $(shell uname -m)
PLATFORM := manylinux2014_${ARCH}
PACKAGE_VERSION ?= 0.0.0

STREAMER_MOCK_LIB = ../../cpp/bazel-bin/mock/libstreamer-mock.so

.PHONY: install_dev_dep build mock test clean

install_dev_dep:
	python3 -m pip install -r requirements.dev

build:
	python3 setup.py bdist_wheel --plat-name ${PLATFORM}

mock:
	make -C ../../cpp build_mock

test-unit: mock install_dev_dep
	STREAMER_LIBRARY=$(STREAMER_MOCK_LIB) python3 -m unittest discover -b runai_model_streamer

test-dist: mock install_dev_dep
	torchrun --nproc_per_node 2 run_dist_tests.py

test: test-unit test-dist

clean:
	rm -rf build/ dist/ runai_model_streamer.egg-info/

install:
	pip3 install --force-reinstall dist/runai_model_streamer-${PACKAGE_VERSION}-py3-none-${PLATFORM}.whl
