RUN_TIMES := 10
MINIO_API_PORT := 9000
MINIO_DATA_DIR := /tmp/minio
MINIO_BUCKET := test-bucket
MINIO_ROOT_USER := minioadmin
MINIO_ROOT_PASSWORD := minioadmin

.PHONY: s3 fuzzing all

s3:
	@echo "Starting MinIO server on port ${MINIO_API_PORT} with data directory ${MINIO_DATA_DIR}"; \
	mkdir -p ${MINIO_DATA_DIR}/${MINIO_BUCKET}; \
	MINIO_ROOT_USER=${MINIO_ROOT_USER} MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD} minio server ${MINIO_DATA_DIR} --address ":${MINIO_API_PORT}" & \
	MINIO_PID=$$!; \
	echo "MinIO PID is $$MINIO_PID"; \
	RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=0 AWS_BUCKET=${MINIO_BUCKET} AWS_ENDPOINT_URL=http://127.0.0.2:${MINIO_API_PORT} AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER} AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD} pytest s3/; \
	echo "Stopping MinIO..."; \
	kill $$MINIO_PID; \
	wait $$MINIO_PID 2>/dev/null || true; \
	rm -rf ${MINIO_DATA_DIR}

fuzzing:
	for i in $(shell seq 1 ${RUN_TIMES}); \
	do \
		echo "Running test iteration $$i"; \
		pytest fuzzing/; \
	done

all: fuzzing s3
