import unittest
import tempfile
import shutil
import os
import random
import string
from safetensors.torch import safe_open
from tests.safetensors.generator import create_random_safetensors
from tests.safetensors.comparison import tensor_maps_are_equal
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

def compatibility_test_cases(backend_class, scheme, bucket_name):
    class TestObjectStorageCompatibility(unittest.TestCase):
        def setUp(self):
            self.temp_dir = tempfile.mkdtemp()
            self.server = backend_class()
            self.bucket_name = bucket_name
            self.scheme = scheme
            self.server.wait_for_startup()

        def test_safetensors_streamer(self):
            file_path = create_random_safetensors(self.temp_dir)
            self.server.upload_file(self.bucket_name, "", file_path)

            our = {}
            with SafetensorsStreamer() as run_sf:
                run_sf.stream_file(f"{self.scheme}://{self.bucket_name}/model.safetensors", None, "cpu")
                for name, tensor in run_sf.get_tensors():
                    our[name] = tensor

            their = {}
            with safe_open(file_path, framework="pt", device="cpu") as f:
                for name in f.keys():
                    their[name] = f.get_tensor(name)

            equal, message = tensor_maps_are_equal(our, their)
            if not equal:
                self.fail(f"Tensor mismatch: {message}")
        
        def test_safetensors_truncated_file_body(self):
            """
            Tests the scenario where the header is valid, but the file ends unexpectedly 
            (EOF) while reading the tensor data.
            """
            import struct
            import json

            # 1. Manually craft a corrupted file
            # Define a header expecting 100 bytes of data
            header = {
                "tensor_truncated": {
                    "dtype": "U8",
                    "shape": [100000000],
                    "data_offsets": [0, 100000000]
                }
            }
            header_json = json.dumps(header).encode('utf-8')
            
            filename = f"truncated_{random_letters(5)}.safetensors"
            file_path = os.path.join(self.temp_dir, filename)
            
            with open(file_path, "wb") as f:
                # Write 8-byte header length
                f.write(struct.pack('<Q', len(header_json)))
                # Write Header
                f.write(header_json)
                # Write Data: Only write 10 bytes instead of the expected 100
                f.write(b'\x00' * 10)

            # 2. Upload the corrupted file
            self.server.upload_file(self.bucket_name, "", file_path)

            # 3. Stream and expect a ValueError during iteration
            with SafetensorsStreamer() as run_sf:
                # The stream_file call might succeed (it only reads the header),
                # but the iteration MUST fail when it hits the EOF in the body.
                run_sf.stream_file(f"{self.scheme}://{self.bucket_name}/{filename}", None, "cpu")
                
                with self.assertRaises(ValueError):
                    for name, tensor in run_sf.get_tensors():
                        pass

        def test_list_files(self):
            file_paths = [create_random_files(self.temp_dir) for _ in range(FILE_COUNT)]

            directory = random_letters(10)
            for file_path in file_paths:
                self.server.upload_file(self.bucket_name, directory, file_path)

            safetensors_files = [f"{self.scheme}://{self.bucket_name}/{directory}/{os.path.basename(fp)}" for fp in file_paths if fp.endswith('.safetensors')]
            
            result_files = list_safetensors(f"{self.scheme}://{self.bucket_name}/{directory}/")
            self.assertEqual(sorted(result_files), sorted(safetensors_files))

        def test_list_files_is_not_recursive(self):
            file_paths = [create_random_files(self.temp_dir) for _ in range(FILE_COUNT)]

            directory = random_letters(10).rstrip("/")
            subdirectory = directory + "/" + random_letters(10).rstrip("/")
            for file_path in file_paths:
                self.server.upload_file(self.bucket_name, subdirectory, file_path)

            safetensors_files = [f"{self.scheme}://{self.bucket_name}/{subdirectory}/{os.path.basename(fp)}" for fp in file_paths if fp.endswith('.safetensors')]
            
            result_files = list_safetensors(f"{self.scheme}://{self.bucket_name}/{directory}/")
            self.assertEqual(len(result_files), 0)
            result_files = list_safetensors(f"{self.scheme}://{self.bucket_name}/{subdirectory}")
            self.assertEqual(sorted(result_files), sorted(safetensors_files))

        def test_pull_files(self):
            file_paths = [create_random_files(self.temp_dir) for _ in range(FILE_COUNT)]
            directory = random_letters(10)
            for file_path in file_paths:
                self.server.upload_file(self.bucket_name, directory, file_path)

            pull_dir = tempfile.mkdtemp()

            pull_files(f"{self.scheme}://{self.bucket_name}/{directory}", pull_dir, ignore_pattern=[
                                                    "*.pt", "*.safetensors", "*.bin"
                                                ])

            pulled_files = os.listdir(pull_dir)
            original_files = [os.path.basename(fp) for fp in file_paths if not (fp.endswith("pt") or fp.endswith("safetensors") or fp.endswith("bin"))]

            self.assertEqual(sorted(pulled_files), sorted(original_files))

        def tearDown(self):
            shutil.rmtree(self.temp_dir)

    return TestObjectStorageCompatibility