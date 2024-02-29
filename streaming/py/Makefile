mock:
	make -C ../cpp build_mock

test: mock
	STREAMER_LIBRARY="../cpp/bazel-bin/mock/streamer-mock.so" python3 -m unittest -b streamer/test_streamer.py