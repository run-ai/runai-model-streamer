import unittest
import os
import time
import boto3

from botocore.exceptions import NoCredentialsError, ClientError

from tests.cases.interface import ObjectStoreBackend
from tests.cases.testcases import compatibility_test_cases


class MinioServer(ObjectStoreBackend):
    def __init__(self):
        self.url = os.getenv("AWS_ENDPOINT_URL")
        self.key = os.getenv("AWS_ACCESS_KEY_ID")
        self.password = os.getenv("AWS_SECRET_ACCESS_KEY")

    def wait_for_startup(self, timeout=30):
        print("Waiting for MinIO server to be up and running.")
        start_time = time.time()
        s3_client = boto3.client(
            "s3",
            endpoint_url=self.url,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.password
        )
        while time.time() - start_time < timeout:
            try:
                s3_client.list_buckets()
                print("MinIO server is up and running.")
                return
            except (ClientError, NoCredentialsError):
                time.sleep(0.5)
        raise TimeoutError(f"MinIO server failed to start within {timeout} seconds.")

    def upload_file(self, bucket, directory, file):
        s3_client = boto3.client(
            's3',
            endpoint_url=self.url,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.password
        )
        s3_client.upload_file(file, bucket, os.path.join(directory, os.path.basename(file)))

TestS3ompatibility = compatibility_test_cases(
    backend_class = MinioServer,
    scheme = "s3",
    bucket_name = os.getenv("AWS_BUCKET")
)

if __name__ == "__main__":
    unittest.main()
