import unittest
import tempfile
import shutil
import os
import torch
from safetensors.torch import safe_open
from tests.safetensors.generator import (create_random_safetensors, create_random_multi_safetensors)
from tests.safetensors.comparison import tensor_maps_are_equal
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

MIN_NUM_FILES = 1
MAX_NUM_FILES = 20

class TestSafetensorStreamerFuzzing(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_safetensors_streamer(self):
        file_path = create_random_safetensors(self.temp_dir)

        file_size = os.path.getsize(file_path)

        our = {}
        with SafetensorsStreamer() as run_sf:
            run_sf.stream_file(file_path, None, "cpu", False)
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        their = {}
        with safe_open(file_path, framework="pt", device="cpu") as f:
            for name in f.keys():
                their[name] = f.get_tensor(name)

        equal, message = tensor_maps_are_equal(our, their)
        if not equal:
            self.fail(f"Tensor mismatch: {message}")

    def test_safetensors_streamer_stream_files(self):
        file_paths = create_random_multi_safetensors(self.temp_dir)

        files_size = sum([os.path.getsize(file_path) for file_path in file_paths])

        our = {}
        with SafetensorsStreamer() as run_sf:
            run_sf.stream_files(file_paths)
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        their = {}
        for file_path in file_paths:
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

    def test_truncated_safetensors_file(self):
        """
        Tests that a valid safetensors file which has been physically truncated
        (EOF earlier than header expects) raises a ValueError.
        """
        # 1. Create a valid random file
        file_path = create_random_safetensors(self.temp_dir)
        original_size = os.path.getsize(file_path)

        # 2. Forcefully truncate the file to 50% of its size
        # This guarantees the file is smaller than the header claims.
        truncated_size = original_size // 2
        with open(file_path, "a") as f:
            f.truncate(truncated_size)
        
        # 3. Assert that processing the file throws a ValueError
        with SafetensorsStreamer() as run_sf:
            with self.assertRaises(ValueError):
                # The error might happen here (if header is truncated)
                run_sf.stream_file(file_path, None, "cpu", False)
                
                # OR it must happen here (if header is okay but body is missing)
                for _ in run_sf.get_tensors():
                    pass

    def tearDown(self):
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
