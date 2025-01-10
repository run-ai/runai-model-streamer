ARCH := $(shell uname -m)
PLATFORM := manylinux2014_${ARCH}
PACKAGE_VERSION ?= 0.0.0

.PHONY: build clean

build:
	python3 setup.py bdist_wheel --plat-name ${PLATFORM}

clean:
	rm -rf build/ dist/ runai_model_streamer_s3.egg-info/

install:
	pip3 install --force-reinstall dist/runai_model_streamer_s3-${PACKAGE_VERSION}-py3-none-${PLATFORM}.whl --break-system-packages
