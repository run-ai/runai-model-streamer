install_dep:
	python3 -m pip install -r requirements.txt

install_dev_dep:
	python3 -m pip install -r requirements.dev

build: install_dep
	python3 setup.py bdist_wheel

mock:
	make -C ../cpp build_mock

test: mock install_dep install_dev_dep
	STREAMER_LIBRARY="../cpp/bazel-bin/mock/libstreamer-mock.so" python3 -m unittest discover -b streamer
