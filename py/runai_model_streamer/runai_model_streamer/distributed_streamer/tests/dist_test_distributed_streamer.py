import unittest
import os
import ctypes
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
)
from runai_model_streamer.file_streamer.requests_iterator import (
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
            
        if self.world_size < 2:
            self.skipTest("Alignment packing test requires at least 2 processes.")

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

                for _req_id, _chunk_idx, data_tensor in streamer.get_chunks():
                    ptr = data_tensor.data_ptr()
                    self.assertEqual(
                        ptr % alignment, 0,
                        f"Rank {self.rank}: tensor data_ptr 0x{ptr:x} is not aligned to {alignment} bytes"
                    )

        if self.rank == 0:
            print(f"\n✅ Alignment test verified on all {self.world_size} ranks.")

    def test_1_auto_mode_no_distributed_with_gloo(self):
        """RUNAI_STREAMER_DIST=auto should disable distributed streaming for gloo backend."""
        if self.world_size < 2:
            self.skipTest("This test requires at least 2 processes.")

        if dist.get_backend() != "gloo":
            self.skipTest("This test requires gloo backend to be initialized")

        file_specs = [{"size": 100, "chunks": [100]}]
        requests = self._prepare_file_requests(file_specs)
        original_data_map = {}
        for req in requests:
            with open(req.path, "rb") as f:
                original_data_map[req.id] = f.read()

        with patch.dict(os.environ, {"RUNAI_STREAMER_DIST": "auto"}):
            with DistributedStreamer() as streamer:
                streamer.stream_files(requests, None, "cpu", True)
                self.assertFalse(
                    streamer.is_distributed,
                    "Distributed streaming should be disabled for gloo backend when RUNAI_STREAMER_DIST=auto"
                )
                reconstructed_data_map = {req.id: [None] * len(req.chunks) for req in requests}
                for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                    reconstructed_data_map[req_id][chunk_idx] = data_tensor.cpu().numpy().tobytes()

        for req in requests:
            reconstructed_bytes = b"".join(reconstructed_data_map[req.id])
            self.assertEqual(original_data_map[req.id], reconstructed_bytes)

        if self.rank == 0:
            print(f"\n✅ Auto mode correctly disabled distributed streaming for gloo backend.")

    def test_9_failure_on_one_rank(self):
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

    def _load_cuda_mock(self):
        """Load libcuda.so.1 and return it if it is the test mock (has call counter).

        Returns None if libcuda.so.1 is not found or is the real CUDA driver
        (which does not export get_cuMemcpyHtoDAsync_call_count).  Tests that
        call this helper must skip when it returns None.
        """
        try:
            lib = ctypes.CDLL("libcuda.so.1")
            lib.get_cuMemcpyHtoDAsync_call_count.restype = ctypes.c_int
            # Accessing the symbol raises AttributeError on real libcuda.so.1
            _ = lib.reset_cuMemcpyHtoDAsync_call_count
            return lib
        except Exception:
            return None

    def _wrap_get_chunks_as_tensors(self, streamer):
        """Wrap file_streamer.get_chunks to convert numpy slices to CPU torch tensors.

        When _use_cuda_direct is mocked, the file_streamer reads to a CPU numpy
        buffer (device="cpu"), but _get_chunks_cuda expects torch.Tensors so it
        can call .numel() and pass them to dist.broadcast.  This wrapper sits
        between the two without touching any production code.
        """
        orig_get_chunks = streamer.distributed_streamer.file_streamer.get_chunks
        def _tensor_get_chunks():
            for req_id, chunk_idx, chunk in orig_get_chunks():
                if not isinstance(chunk, torch.Tensor):
                    chunk = torch.as_tensor(chunk)
                yield req_id, chunk_idx, chunk
        streamer.distributed_streamer.file_streamer.get_chunks = _tensor_get_chunks

    def test_1_success_cuda_direct_data_correctness(self):
        """Test _get_chunks_cuda tensor-by-tensor broadcast with fixed file specs.

        Mocks _use_cuda_direct to force the CUDA direct path and patches
        torch.cuda.synchronize so the test runs without GPU hardware.
        Uses CPU tensors throughout; data correctness is verified end-to-end.
        """
        file_specs = [{"size": 2580, "chunks": [500, 260, 260, 260, 260, 260, 260, 260, 260]}]
        requests = self._prepare_file_requests(file_specs)
        original_data_map = {}
        for req in requests:
            with open(req.path, "rb") as f:
                original_data_map[req.id] = f.read()

        reconstructed_data_map = {req.id: [None] * len(req.chunks) for req in requests}
        env_vars = {"RUNAI_STREAMER_DIST": "1", "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": "0"}
        with patch.dict(os.environ, env_vars), \
             patch("runai_model_streamer.distributed_streamer.distributed_streamer._distributedStreamer._use_cuda_direct", return_value=True), \
             patch("torch.cuda.synchronize"):
            with DistributedStreamer() as streamer:
                streamer.stream_files(requests, None, "cpu", True)
                self._wrap_get_chunks_as_tensors(streamer)
                for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                    reconstructed_data_map[req_id][chunk_idx] = data_tensor.cpu().numpy().tobytes()

        for req in requests:
            reconstructed_bytes = b"".join(reconstructed_data_map[req.id])
            self.assertEqual(original_data_map[req.id], reconstructed_bytes)

        if self.rank == 0:
            print(f"\n✅ CUDA direct data correctness test verified on all {self.world_size} ranks.")

    def test_1_success_cuda_direct_random_files(self):
        """Test _get_chunks_cuda tensor-by-tensor broadcast with random file/chunk sizes."""
        num_files = random.randint(1, 10)
        file_specs = []
        for _ in range(num_files):
            total_size = random.randint(1, 1 * 1024 * 1024)
            chunks = []
            remaining = total_size
            while remaining > 0:
                chunk = random.randint(1, min(remaining, 256 * 1024))
                chunks.append(chunk)
                remaining -= chunk
            file_specs.append({"size": total_size, "chunks": chunks})

        requests = self._prepare_file_requests(file_specs)
        original_data_map = {}
        for req in requests:
            with open(req.path, "rb") as f:
                original_data_map[req.id] = f.read()

        reconstructed_data_map = {req.id: [None] * len(req.chunks) for req in requests}
        env_vars = {"RUNAI_STREAMER_DIST": "1", "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": "0"}
        with patch.dict(os.environ, env_vars), \
             patch("runai_model_streamer.distributed_streamer.distributed_streamer._distributedStreamer._use_cuda_direct", return_value=True), \
             patch("torch.cuda.synchronize"):
            with DistributedStreamer() as streamer:
                streamer.stream_files(requests, None, "cpu", True)
                self._wrap_get_chunks_as_tensors(streamer)
                for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                    reconstructed_data_map[req_id][chunk_idx] = data_tensor.cpu().numpy().tobytes()

        for req in requests:
            reconstructed_bytes = b"".join(reconstructed_data_map[req.id])
            self.assertEqual(original_data_map[req.id], reconstructed_bytes)

        if self.rank == 0:
            print(f"\n✅ CUDA direct random files test verified on all {self.world_size} ranks.")

    def test_1_success_cuda_direct_verifies_mock(self):
        """Verify that the CUDA direct path actually calls cuMemcpyHtoDAsync.

        Loads libcuda.so.1 via ctypes and checks the call counter exported by
        the mock.  Skips automatically when the mock is not in LD_LIBRARY_PATH
        (real GPU machine or no mock built) — identical skip logic to
        ReadCuda/Sanity in batch_cuda_test.cc.

        torch.empty is redirected to CPU so no real GPU is needed; data
        correctness is verified end-to-end alongside the counter check.
        """
        if self.world_size < 2:
            self.skipTest("Distributed CUDA direct test requires at least 2 processes.")

        cuda_mock = self._load_cuda_mock()
        if cuda_mock is None:
            self.skipTest("Mock libcuda.so.1 not available — set LD_LIBRARY_PATH to the mock build directory.")

        file_specs = [{"size": 2580, "chunks": [500, 260, 260, 260, 260, 260, 260, 260, 260]}]
        requests = self._prepare_file_requests(file_specs)
        original_data_map = {}
        for req in requests:
            with open(req.path, "rb") as f:
                original_data_map[req.id] = f.read()

        cuda_mock.reset_cuMemcpyHtoDAsync_call_count()

        # Redirect torch.empty(device="cuda:X") to CPU so tests run without a GPU.
        _orig_empty = torch.empty
        def _cpu_empty(*args, **kwargs):
            if str(kwargs.get("device", "")).startswith("cuda"):
                kwargs = {**kwargs, "device": "cpu"}
            return _orig_empty(*args, **kwargs)

        reconstructed_data_map = {req.id: [None] * len(req.chunks) for req in requests}
        env_vars = {"RUNAI_STREAMER_DIST": "1", "RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE": "0"}
        with patch.dict(os.environ, env_vars), \
             patch("torch.cuda.is_available", return_value=True), \
             patch("torch.empty", side_effect=_cpu_empty), \
             patch("torch.cuda.mem_get_info", return_value=(2**33, 2**33)), \
             patch("torch.cuda.synchronize"):
            with DistributedStreamer() as streamer:
                # Bypass the gloo+non-cpu backend check in set_is_distributed so
                # distributed streaming is active even with device="cuda:0".
                def _force_distributed(is_distributed, device):
                    streamer.is_distributed = (
                        is_distributed
                        and dist.is_initialized()
                        and dist.get_world_size() > 1
                    )
                with patch.object(streamer, "set_is_distributed", side_effect=_force_distributed):
                    streamer.stream_files(requests, None, "cuda:0", True)
                for req_id, chunk_idx, data_tensor in streamer.get_chunks():
                    reconstructed_data_map[req_id][chunk_idx] = data_tensor.cpu().numpy().tobytes()

        call_count = cuda_mock.get_cuMemcpyHtoDAsync_call_count()
        self.assertGreater(
            call_count, 0,
            f"Rank {self.rank}: expected cuMemcpyHtoDAsync to be called but count was {call_count}"
        )

        for req in requests:
            reconstructed_bytes = b"".join(reconstructed_data_map[req.id])
            self.assertEqual(original_data_map[req.id], reconstructed_bytes)

        if self.rank == 0:
            print(f"\n✅ CUDA mock verified: cuMemcpyHtoDAsync called {call_count} times, data correct on all {self.world_size} ranks.")


if __name__ == '__main__':
    unittest.main()