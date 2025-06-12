import unittest
import tempfile
import shutil
import os
import torch
from safetensors.torch import safe_open
from tests.safetensors_generator.safetensors_generator import create_random_safetensors
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

class TestSafetensorStreamerFuzzing(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def test_safetensors_streamer(self):
        file_path = os.path.join(self.temp_dir, "model.safetensors")
        create_random_safetensors(file_path)

        file_size = os.path.getsize(file_path)

        print(f"File size: {file_size} bytes")

        our = {}
        with SafetensorsStreamer() as run_sf:
            run_sf.stream_file(file_path)
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
        shutil.rmtree(self.temp_dir)


if __name__ == "__main__":
    unittest.main()
