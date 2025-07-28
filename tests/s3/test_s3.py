import unittest
import tempfile
import shutil
import os
import time
import boto3
import random
import string
from safetensors.torch import safe_open
from tests.safetensors.generator import create_random_safetensors
from tests.safetensors.comparison import tensor_maps_are_equal
from botocore.exceptions import NoCredentialsError, ClientError
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
    list_safetensors,
    pull_files
)

METADATA_SUFFIX = ['safetensors', 'json', 'config', 'xml', 'pt', 'bin']
FILE_COUNT = 5

def random_letters(x):
    return ''.join(random.choices(string.ascii_letters, k=x))

def create_random_files(dir):
    file_path = os.path.join(dir, f"{random_letters(5)}.{random.choice(METADATA_SUFFIX)}")
    with open(file_path, "w") as file:
        file.write(random_letters(15))
    return file_path


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

    def upload_file_to_minio(self, bucket, directory, file):
        s3_client = boto3.client(
            's3',
            endpoint_url=self.url,
            aws_access_key_id=self.key,
            aws_secret_access_key=self.password
        )
        s3_client.upload_file(file, bucket, os.path.join(directory, os.path.basename(file)))

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
        self.minio_server.upload_file_to_minio(self.s3_bucket, "", file_path)

        our = {}
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
            
    def test_list_files(self):
        file_paths = [create_random_files(self.temp_dir) for _ in range(FILE_COUNT)]

        directory = random_letters(10)
        for file_path in file_paths:
            self.minio_server.upload_file_to_minio(self.s3_bucket, directory, file_path)

        safetensors_files = [f"s3://{self.s3_bucket}/{directory}/{os.path.basename(fp)}" for fp in file_paths if fp.endswith('.safetensors')]
        
        result_files = list_safetensors(f"s3://{self.s3_bucket}/{directory}")
        self.assertEqual(sorted(result_files), sorted(safetensors_files))

    def test_pull_files(self):
        file_paths = [create_random_files(self.temp_dir) for _ in range(FILE_COUNT)]
        directory = random_letters(10)
        for file_path in file_paths:
            self.minio_server.upload_file_to_minio(self.s3_bucket, directory, file_path)

        pull_dir = tempfile.mkdtemp()

        pull_files(f"s3://{self.s3_bucket}/{directory}", pull_dir, ignore_pattern=[
                                        "*.pt", "*.safetensors", "*.bin"
                                    ])

        pulled_files = os.listdir(pull_dir)
        original_files = [os.path.basename(fp) for fp in file_paths if not (fp.endswith("pt") or fp.endswith("safetensors") or fp.endswith("bin"))]

        self.assertEqual(sorted(pulled_files), sorted(original_files))

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
