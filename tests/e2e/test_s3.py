import unittest
import tempfile
import shutil
import os
import time
import boto3
import subprocess
import torch
from safetensors.torch import safe_open
from tests.safetensors_generator.safetensors_generator import create_random_safetensors
from botocore.exceptions import NoCredentialsError, ClientError
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

MINIO_EXECUTABLE_PATH = "minio"
MINIO_API_PORT = 9000
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT_URL = f"http://127.0.0.1:{MINIO_API_PORT}"
MINIO_BUCKET_NAME = "test-bucket"

def wait_for_minio(process, timeout=30):
    print("Waiting for MinIO server to be up and running.")
    start_time = time.time()
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    while time.time() - start_time < timeout:
        if process.poll() is not None:
             raise RuntimeError("MinIO process terminated unexpectedly during startup.")
        try:
            s3_client.list_buckets()
            print("MinIO server is up and running.")
            return
        except (ClientError, NoCredentialsError):
            time.sleep(0.5)
    raise TimeoutError(f"MinIO server failed to start within {timeout} seconds.")

def start_minio(data_dir):
    print(f"Starting MinIO with data_dir: {data_dir}")
    os.mkdir(os.path.join(data_dir, MINIO_BUCKET_NAME))
    minio_env = os.environ.copy()
    minio_env["MINIO_ROOT_USER"] = MINIO_ACCESS_KEY
    minio_env["MINIO_ROOT_PASSWORD"] = MINIO_SECRET_KEY

    command = [
        MINIO_EXECUTABLE_PATH,
        "server",
        data_dir,
        f"--address=:{MINIO_API_PORT}",
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=minio_env
    )
    wait_for_minio(process)
    return process

def stop_minio(process):
    print("Terminating MinIO")
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print("MinIO did not terminate gracefully, forcing shutdown...")
        process.kill()

def upload_file_to_minio(file):
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    s3_client.upload_file(file, MINIO_BUCKET_NAME, os.path.basename(file))

class TestS3Compatibility(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.minio_process = start_minio(self.temp_dir)

    def test_safetensors_streamer(self):
        file_path = os.path.join(self.temp_dir, "model.safetensors")
        create_random_safetensors(file_path)
        upload_file_to_minio(file_path)

        our = {}
        os.environ["RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING"] = "0"
        os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT_URL
        os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
        os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY

        with SafetensorsStreamer() as run_sf:
            run_sf.stream_file(f"s3://{MINIO_BUCKET_NAME}/model.safetensors")
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        their = {}
        with safe_open(file_path, framework="pt", device="cpu") as f:
            for name in f.keys():
                their[name] = f.get_tensor(name)

        self.assertEqual(len(our.items()), len(their.items()))
        for name, our_tensor in our.items():
            self.assertTrue(our_tensor.is_contiguous())
            self.assertEqual(our_tensor.dtype, their[name].dtype)
            self.assertEqual(our_tensor.shape, their[name].shape)
            res = torch.all(our_tensor.eq(their[name]))
            self.assertTrue(res)

    def tearDown(self):
        stop_minio(self.minio_process)
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
