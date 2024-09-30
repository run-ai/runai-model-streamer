import unittest
import tempfile
import shutil
import os
import random
import string
import torch
from safetensors.torch import save_file, safe_open
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

MIN_NUM_TENSORS = 1
MAX_NUM_TENSORS = 5
MIN_TENSOR_SIZE = 16
MAX_TENSOR_SIZE = 2048
MIN_TENSOR_NAME_LEN = 1
MAX_TENSOR_NAME_LEN = 248
MIN_TENSOR_SHAPE_DIM = 1
MAX_TENSOR_SHAPE_DIM = 10
ST_DATA_TYPES = [
    torch.float64,
    torch.float32,
    torch.float16,
    torch.bfloat16,
    torch.int64,
    torch.int32,
    torch.int16,
    torch.int8,
    torch.uint8,
    torch.bool,
]


def random_name():
    characters = string.ascii_letters + string.digits + "."
    return "".join(
        random.choice(characters)
        for _ in range(random.randint(MIN_TENSOR_NAME_LEN, MAX_TENSOR_NAME_LEN))
    )


def random_tensors():
    tensors = {}
    for i in range(random.randint(MIN_NUM_TENSORS, MAX_NUM_TENSORS)):
        dtype = random.shuffle(ST_DATA_TYPES)
        shape = tuple(
            random.randint(MIN_TENSOR_SHAPE_DIM, MAX_TENSOR_SHAPE_DIM)
            for _ in range(random.randint(MIN_TENSOR_SHAPE_DIM, MAX_TENSOR_SHAPE_DIM))
        )
        name = random_name()

        if dtype in [torch.int32, torch.int64]:
            tensor = torch.randint(low=0, high=100, size=shape, dtype=dtype)
        else:
            tensor = torch.rand(size=shape, dtype=dtype)

        tensors[name] = tensor
    return tensors


def create_random_safetensors(path):
    save_file(random_tensors(), path)


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
