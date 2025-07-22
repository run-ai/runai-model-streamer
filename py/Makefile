.PHONY: build test clean

build:
	make -C runai_model_streamer build
	make -C runai_model_streamer_gcs build
	make -C runai_model_streamer_s3 build

test:
	make -C runai_model_streamer test

clean:
	make -C runai_model_streamer clean
	make -C runai_model_streamer_gcs clean
	make -C runai_model_streamer_s3 clean

install:
	make -C runai_model_streamer install
	make -C runai_model_streamer_gcs install
	make -C runai_model_streamer_s3 install
