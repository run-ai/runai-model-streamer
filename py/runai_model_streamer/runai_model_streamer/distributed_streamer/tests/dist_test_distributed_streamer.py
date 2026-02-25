import unittest
import os
import torch
import torch.distributed as dist
from typing import List
import tempfile
import shutil
import pickle
import time
import random
from unittest.mock import patch

from runai_model_streamer.distributed_streamer.distributed_streamer import (
    DistributedStreamer,
    RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR,
)
from runai_model_streamer.file_streamer import FileChunks

class TestDistributedStreamer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.rank = dist.get_rank()
        cls.world_size = dist.get_world_size()

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
            
    def _prepare_file_requests(self, file_specs: List[dict]) -> List[FileChunks]:
        # rank 0 creates the file requests and broadcasts them to all ranks
        if self.rank == 0:
            file_requests = []
            for i, spec in enumerate(file_specs):
                file_path = os.path.join(self.temp_dir, f"file_{i}.bin")
                content = torch.randint(0, 256, (spec["size"],), dtype=torch.uint8).numpy().tobytes()
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
        file_specs = [{"size": 2580, "chunks": [500, 260, 260, 260, 260, 260, 260, 260, 260]}]
        requests = self._prepare_file_requests(file_specs)
        original_data_map = {}
        for req in requests:
            with open(req.path, "rb") as f:
                original_data_map[req.id] = f.read()
        reconstructed_data_map = {req.id: [None] * len(req.chunks) for req in requests}
        env_vars = {"RUNAI_STREAMER_DIST": "1", "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": "0"}
        with patch.dict(os.environ, env_vars):
            with DistributedStreamer() as streamer:
                streamer.stream_files(requests, None, "cpu", True)
                for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                    reconstructed_data_map[req_id][chunk_idx] = data_tensor.cpu().numpy().tobytes()
            for req_id, chunks in reconstructed_data_map.items():
                reconstructed_bytes = b"".join(chunks)
                self.assertEqual(original_data_map[req.id], reconstructed_bytes)
            if self.rank == 0:
                print(f"\n✅ Success test verified on all {self.world_size} ranks.")

    def test_1_success_empty_file_list(self):
        # This test is correct and remains unchanged
        requests = []
        env_vars = {"RUNAI_STREAMER_DIST": "1"}
        with patch.dict(os.environ, env_vars):
            with DistributedStreamer() as streamer:
                streamer.stream_files(requests, None, "cpu", True)
                count = 0
                for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                    count += 1
            self.assertEqual(count, 0)
        if self.rank == 0:
            print(f"\n✅ Success empty file list test verified on all {self.world_size} ranks.")

    def test_1_success_random_files(self):
        # --- MODIFIED SECTION: Generate random file specifications ---
        file_specs = []
        # Use a random number of files (1 to 10)
        num_files = random.randint(1, 10)

        for _ in range(num_files):
            # Use a random file size between 0 and 10 MB
            total_size = random.randint(0, 10 * 1024 * 1024)
            
            # Handle the edge case of a 0-byte file
            if total_size == 0:
                file_specs.append({"size": 0, "chunks": []})
                continue

            # Generate random chunk sizes that sum up to the total file size
            chunks = []
            remaining_size = total_size
            while remaining_size > 0:
                # Set a max chunk size for realism (e.g., 1MB), but don't exceed what's left
                max_chunk_size = min(remaining_size, 1 * 1024 * 1024)
                chunk_size = random.randint(1, max_chunk_size)
                chunks.append(chunk_size)
                remaining_size -= chunk_size
            
            file_specs.append({"size": total_size, "chunks": chunks})
        # --- END OF MODIFIED SECTION ---

        requests = self._prepare_file_requests(file_specs)
        original_data_map = {}
        for req in requests:
            with open(req.path, "rb") as f:
                original_data_map[req.id] = f.read()

        reconstructed_data_map = {req.id: [None] * len(req.chunks) for req in requests}
        env_vars = {"RUNAI_STREAMER_DIST": "1", "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": "0"}
        with patch.dict(os.environ, env_vars):
            with DistributedStreamer() as streamer:
                streamer.stream_files(requests, None, "cpu", True)
                for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                    reconstructed_data_map[req_id][chunk_idx] = data_tensor.cpu().numpy().tobytes()

            # Verify that the reconstructed data for each file matches the original
            for req in requests:
                reconstructed_bytes = b"".join(reconstructed_data_map[req.id])
                self.assertEqual(original_data_map[req.id], reconstructed_bytes)

        if self.rank == 0:
            print(f"\n✅ Success random files test verified on all {self.world_size} ranks.")

    def test_1_success_alignment(self):
        # Chunk sizes are random in [1, alignment*10].
        # BUFFER_MIN_BYTESIZE = alignment*20 guarantees at least two max-size chunks fit in
        # one batch: worst case is chunk1=alignment*10 at offset 0 and chunk2=alignment*10
        # at offset=alignment*10, totalling alignment*20 = BUFFER_BYTESIZE exactly.
        # With 1-20 files of 4 chunks each there are enough chunks to guarantee at least
        # one batch where two tensors are packed together.
        alignment = 512
        num_files = random.randint(1, 20)
        def random_chunks(n):
            return [random.randint(1, alignment * 10) for _ in range(n)]

        chunks_per_file = [random_chunks(4) for _ in range(num_files)]
        file_specs = [
            {"size": sum(c), "chunks": c} for c in chunks_per_file
        ]
        requests = self._prepare_file_requests(file_specs)

        env_vars = {
            "RUNAI_STREAMER_DIST": "1",
            "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": str(alignment * 20),
            RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR: str(alignment),
        }
        with patch.dict(os.environ, env_vars):
            with DistributedStreamer() as streamer:
                streamer.stream_files(requests, None, "cpu", True)

                storage_ptr_counts = {}
                for _req_id, _chunk_idx, data_tensor in streamer.get_chunks():
                    ptr = data_tensor.data_ptr()
                    self.assertEqual(
                        ptr % alignment, 0,
                        f"Rank {self.rank}: tensor data_ptr 0x{ptr:x} is not aligned to {alignment} bytes"
                    )
                    storage_ptr = data_tensor.storage().data_ptr()
                    storage_ptr_counts[storage_ptr] = storage_ptr_counts.get(storage_ptr, 0) + 1

                self.assertTrue(
                    any(count >= 2 for count in storage_ptr_counts.values()),
                    f"Rank {self.rank}: no two tensors were packed in the same buffer batch"
                )

        if self.rank == 0:
            print(f"\n✅ Alignment test verified on all {self.world_size} ranks.")

    def test_9_failure_on_one_rank(self):
        # This test is correct and remains unchanged
        if self.world_size < 2:
            self.skipTest("This failure test requires at least 2 processes.")
        
        file_specs = [{"size": 100, "chunks": [100]} for _ in range(self.world_size)]
        requests = self._prepare_file_requests(file_specs)
        
        env_vars = {"RUNAI_STREAMER_DIST_TIMEOUT": "5", "RUNAI_STREAMER_DIST": "1"}
        with patch.dict(os.environ, env_vars):
            if self.rank == 1:
                file_to_delete = requests[1].path
                if os.path.exists(file_to_delete):
                    os.remove(file_to_delete)
            
            with self.assertRaises(Exception):
                with DistributedStreamer() as streamer:
                    streamer.stream_files(requests, None, "cpu", True)
                    for _ in streamer.get_chunks():
                        pass
        
    def test_9_timeout_failure(self):
        # This test is correct and remains unchanged
        if self.world_size < 2:
            self.skipTest("This timeout test requires at least 2 processes.")

        file_specs = [{"size": 100, "chunks": [100]} for _ in range(self.world_size)]
        requests = self._prepare_file_requests(file_specs)
        
        timeout_seconds = 1
        env_vars = {"RUNAI_STREAMER_DIST_TIMEOUT": str(timeout_seconds), "RUNAI_STREAMER_DIST": "1"}

        with patch.dict(os.environ, env_vars):
            with self.assertRaises(Exception) as context:
                with DistributedStreamer() as streamer:
                    # All ranks must pass the setup phase
                    streamer.stream_files(requests, None, "cpu", True)
                    
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

if __name__ == '__main__':
    unittest.main()