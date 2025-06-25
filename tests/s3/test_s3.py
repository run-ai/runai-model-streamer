import unittest
import tempfile
import shutil
import os
import time
import boto3
from safetensors.torch import safe_open
from tests.safetensors.generator import create_random_safetensors
from tests.safetensors.comparison import tensor_maps_are_equal
from botocore.exceptions import NoCredentialsError, ClientError
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

class MinioServer:
    def __init__(self, url, key, password):
        self.url = url
        self.key = key
        self.password = password

    def wait_for_minio(self, timeout=30):
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

    def upload_file_to_minio(self, bucket, file):
        s3_client = boto3.client(
            's3',
            endpoint_url=self.url,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.password
        )
        s3_client.upload_file(file, bucket, os.path.basename(file))

class TestS3Compatibility(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.s3_bucket = os.getenv("AWS_BUCKET")
        self.s3_url = os.getenv("AWS_ENDPOINT_URL")
        self.s3_key = os.getenv("AWS_ACCESS_KEY_ID")
        self.s3_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.minio_server = MinioServer(
            self.s3_url, 
            self.s3_key, 
            self.s3_secret
        )
        self.minio_server.wait_for_minio()

    def test_safetensors_streamer(self):
        file_path = create_random_safetensors(self.temp_dir)
        self.minio_server.upload_file_to_minio(self.s3_bucket, file_path)

        our = {}
        os.environ["RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING"] = "0"
        os.environ["AWS_ENDPOINT_URL"] = self.s3_url
        os.environ["AWS_ACCESS_KEY_ID"] = self.s3_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.s3_secret

        with SafetensorsStreamer() as run_sf:
            run_sf.stream_file(f"s3://{self.s3_bucket}/model.safetensors")
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        their = {}
        with safe_open(file_path, framework="pt", device="cpu") as f:
            for name in f.keys():
                their[name] = f.get_tensor(name)

        equal, message = tensor_maps_are_equal(our, their)
        if not equal:
            self.fail(f"Tensor mismatch: {message}")

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
