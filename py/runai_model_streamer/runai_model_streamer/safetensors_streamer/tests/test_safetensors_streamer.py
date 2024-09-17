import unittest
import torch
import os
from safetensors import safe_open
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
    convert_path_if_needed,
    RUNAI_DIRNAME,
    RUNAI_DIRNAME_TO_REMOVE,
)


class TestSafetensorsStreamer(unittest.TestCase):
    def test_convert_path_if_needed_local_path(self):
        path = "/a/b/c/d.txt"
        new_path = convert_path_if_needed(path)
        self.assertEqual(path, new_path)

    def test_convert_path_if_needed_s3(self):
        path = "/a/b/c/d.txt"
        os.environ[RUNAI_DIRNAME] = "s3://test-bucket/llama"
        new_path = convert_path_if_needed(path)
        self.assertEqual(new_path, "s3://test-bucket/llama/d.txt")
        os.environ.pop(RUNAI_DIRNAME, None)

    def test_convert_path_if_needed_s3_with_remove(self):
        path = "/a/b/c/d.txt"
        os.environ[RUNAI_DIRNAME] = "s3://test-bucket/llama"
        os.environ[RUNAI_DIRNAME_TO_REMOVE] = "/a/b"
        new_path = convert_path_if_needed(path)
        self.assertEqual(new_path, "s3://test-bucket/llama/c/d.txt")
        os.environ.pop(RUNAI_DIRNAME_TO_REMOVE, None)
        os.environ.pop(RUNAI_DIRNAME, None)

    def test_safetensors_streamer(self):
        print(os.getenv(RUNAI_DIRNAME))
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test.safetensors"
        )
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


if __name__ == "__main__":
    unittest.main()
