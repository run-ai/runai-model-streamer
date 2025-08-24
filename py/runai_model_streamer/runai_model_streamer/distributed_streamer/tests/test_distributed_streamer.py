import unittest
import os
import torch
import torch.distributed as dist
from typing import List
import tempfile
import shutil
import pickle
import time
from unittest.mock import patch

from runai_model_streamer.distributed_streamer.distributed_streamer import DistributedStreamer
from runai_model_streamer.file_streamer import FileChunks

class TestDistributedStreamer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        if not dist.is_initialized():
            dist.init_process_group("gloo")
        cls.rank = dist.get_rank()
        cls.world_size = dist.get_world_size()

    @classmethod
    def tearDownClass(cls):
        if dist.is_initialized():
            dist.barrier()
            dist.destroy_process_group()

    def setUp(self):
        if self.rank == 0:
            self.temp_dir = tempfile.mkdtemp()
        else:
            self.temp_dir = None
        dir_list = [self.temp_dir]
        dist.broadcast_object_list(dir_list, src=0)
        self.temp_dir = dir_list[0]

    def tearDown(self):
        dist.barrier()
        if self.rank == 0 and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            
    def _create_and_broadcast_requests(self, file_specs: List[dict]) -> List[FileChunks]:
        # This helper function is correct and remains unchanged
        if self.rank == 0:
            file_requests = []
            for i, spec in enumerate(file_specs):
                file_path = os.path.join(self.temp_dir, f"file_{i}.bin")
                content = torch.arange(spec["size"], dtype=torch.uint8).numpy().tobytes()
                with open(file_path, "wb") as f:
                    f.write(content)
                file_requests.append(FileChunks(id=i, path=file_path, chunks=spec["chunks"], offset=0))
            data_bytes = pickle.dumps(file_requests)
            size_tensor = torch.tensor([len(data_bytes)], dtype=torch.long)
        else:
            size_tensor = torch.tensor([0], dtype=torch.long)
        dist.broadcast(size_tensor, src=0)
        data_size = size_tensor.item()
        if self.rank == 0:
            buffer = torch.tensor(list(data_bytes), dtype=torch.uint8)
        else:
            buffer = torch.empty(data_size, dtype=torch.uint8)
        dist.broadcast(buffer, src=0)
        if self.rank != 0:
            file_requests = pickle.loads(buffer.numpy().tobytes())
        return file_requests

    def test_1_success_data_correctness(self):
        # This test is correct and remains unchanged
        file_specs = [{"size": 1024, "chunks": [512, 512]}]
        requests = self._create_and_broadcast_requests(file_specs)
        original_data_map = {}
        for req in requests:
            with open(req.path, "rb") as f:
                original_data_map[req.id] = f.read()
        reconstructed_data_map = {req.id: [None] * len(req.chunks) for req in requests}
        with DistributedStreamer() as streamer:
            streamer.stream_files(requests)
            for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                reconstructed_data_map[req_id][chunk_idx] = data_tensor.cpu().numpy().tobytes()
        for req_id, chunks in reconstructed_data_map.items():
            reconstructed_bytes = b"".join(chunks)
            self.assertEqual(original_data_map[req.id], reconstructed_bytes)
        if self.rank == 0:
            print(f"\n✅ Success test verified on all {self.world_size} ranks.")

    def test_9_failure_on_one_rank(self):
        # This test is correct and remains unchanged
        if self.world_size < 2:
            self.skipTest("This failure test requires at least 2 processes.")
        
        file_specs = [{"size": 100, "chunks": [100]} for _ in range(self.world_size)]
        requests = self._create_and_broadcast_requests(file_specs)
        
        env_vars = {"RUNAI_STREAMER_DIST_TIMEOUT": "5"}
        with patch.dict(os.environ, env_vars):
            if self.rank == 1:
                file_to_delete = requests[1].path
                if os.path.exists(file_to_delete):
                    os.remove(file_to_delete)
            
            with self.assertRaises(Exception):
                with DistributedStreamer() as streamer:
                    streamer.stream_files(requests)
                    for _ in streamer.get_chunks():
                        pass
        
    def test_9_timeout_failure(self):
        # This test is correct and remains unchanged
        if self.world_size < 2:
            self.skipTest("This timeout test requires at least 2 processes.")

        file_specs = [{"size": 100, "chunks": [100]} for _ in range(self.world_size)]
        requests = self._create_and_broadcast_requests(file_specs)
        
        timeout_seconds = 1
        env_vars = {"RUNAI_STREAMER_DIST_TIMEOUT": str(timeout_seconds)}

        with patch.dict(os.environ, env_vars):
            with self.assertRaises(Exception) as context:
                with DistributedStreamer() as streamer:
                    # All ranks must pass the setup phase
                    streamer.stream_files(requests)
                    
                    if self.rank == 1:
                        time.sleep(timeout_seconds + 2) # Sleep longer than timeout
                    
                    # All ranks try to get chunks. Rank 0 will time out waiting for rank 1
                    # at the internal barrier/broadcast inside get_chunks.
                    for _ in streamer.get_chunks():
                        pass
            error_message = str(context.exception).lower()
            is_expected_error = "timeout" in error_message or "connection closed by peer" in error_message or "timed out" in error_message
            self.assertTrue(
                is_expected_error,
                f"Rank {self.rank} caught an unexpected error: '{context.exception}'"
            )
            print(f"Rank {self.rank}: Correctly caught expected timeout exception.")

        self.assertNotIn("RUNAI_STREAMER_DIST_TIMEOUT", os.environ)

if __name__ == '__main__':
    unittest.main()