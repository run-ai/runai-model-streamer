build:
	python3 setup.py bdist_wheel

mock:
	make -C ../cpp build_mock

test: mock
	STREAMER_LIBRARY="../cpp/bazel-bin/mock/libstreamer-mock.so" python3 -m unittest discover -b streamer