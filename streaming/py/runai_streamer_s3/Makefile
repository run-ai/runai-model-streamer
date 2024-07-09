ARCH := $(shell uname -m)

build:
	python3 setup.py bdist_wheel --plat-name manylinux1_${ARCH}

clean:
	rm -rf build/ dist/ runai_streamer_s3.egg-info/
