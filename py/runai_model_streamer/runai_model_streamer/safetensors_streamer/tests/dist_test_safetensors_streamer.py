import unittest
import torch
import torch.distributed as dist
import os
import shutil
from safetensors import safe_open
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

class TestDistributedSafetensorsStreamer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.rank = dist.get_rank()
        cls.world_size = dist.get_world_size()

    def setUp(self):
        dist.barrier()

    def tearDown(self):
        dist.barrier()

    def test_distributed_safetensors_streamer(self):
        file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test.safetensors"
        )
        our = {}
        env_vars = {"RUNAI_STREAMER_DIST": "1", "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": "0"}
        with unittest.mock.patch.dict(os.environ, env_vars):
            with SafetensorsStreamer() as run_sf:
                run_sf.stream_file(file_path, None, "cpu", True)
                for name, tensor in run_sf.get_tensors():
                    our[name] = tensor.clone().detach() # because distributed streamer has internal reusable buffer

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


    def test_non_distributed_safetensors_streamer(self):

        rank = dist.get_rank()
        if rank == 0:
            file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "test.safetensors"
            )
        else:
            file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "test_empty.safetensors"
            )

        our = {}
        env_vars = {"RUNAI_STREAMER_DIST": "1", "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": "0"}
        with unittest.mock.patch.dict(os.environ, env_vars):
            with SafetensorsStreamer() as run_sf:
                run_sf.stream_file(file_path, None, "cpu", False)
                for name, tensor in run_sf.get_tensors():
                    our[name] = tensor.clone().detach() # because distributed streamer has internal reusable buffer

        if rank != 0:
            self.assertEqual(len(our.items()), 0)

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
