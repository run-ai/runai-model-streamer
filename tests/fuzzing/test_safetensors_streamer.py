import unittest
import tempfile
import shutil
import os
import torch
from safetensors.torch import safe_open
from tests.safetensors.generator import (create_random_safetensors, create_random_multi_safetensors)
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

MIN_NUM_FILES = 1                                                                                                                                                                                                                                                                                                                                                             
MAX_NUM_FILES = 20    

class TestSafetensorStreamerFuzzing(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def _compare_tensors_robustly(self, our_tensor, their_tensor, name):
        """
        Compares two tensors. Since we use official types, we expect strict bitwise equality.
        """
        self.assertEqual(our_tensor.shape, their_tensor.shape, f"Shape mismatch in {name}")
        self.assertEqual(our_tensor.dtype, their_tensor.dtype, f"Dtype mismatch in {name}")
        
        # View as uint8 to compare raw bits. This is the gold standard for "zero-copy" verification.
        # It ensures we loaded exactly the same bytes as the official library.
        our_bits = our_tensor.detach().cpu().contiguous().view(torch.uint8)
        their_bits = their_tensor.detach().cpu().contiguous().view(torch.uint8)
        
        # We use torch.equal on the uint8 view to verify bit-perfect loading
        self.assertTrue(torch.equal(our_bits, their_bits), f"Bitwise data mismatch in {name}")

    def test_safetensors_streamer(self):
        """Test streaming a single random safetensors file."""
        file_path = create_random_safetensors(self.temp_dir)

        our = {}
        with SafetensorsStreamer() as run_sf:
            run_sf.stream_file(file_path, None, "cpu", False)
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        their = {}
        # This MUST succeed now. If safe_open fails, our generator is producing invalid files.
        with safe_open(file_path, framework="pt", device="cpu") as f:
            for name in f.keys():
                their[name] = f.get_tensor(name)
        
        self.assertEqual(len(our), len(their), "Tensor count mismatch")
        for name in our:
            self._compare_tensors_robustly(our[name], their[name], name)

    def test_safetensors_streamer_stream_files(self):
        """Test streaming multiple random safetensors files."""
        file_paths = create_random_multi_safetensors(self.temp_dir)

        our = {}
        with SafetensorsStreamer() as run_sf:
            run_sf.stream_files(file_paths)
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        their = {}
        for file_path in file_paths:
            # Again, strict dependency on safe_open succeeding
            with safe_open(file_path, framework="pt", device="cpu") as f:
                for name in f.keys():
                    their[name] = f.get_tensor(name)

        self.assertEqual(len(our), len(their), "Total tensor count mismatch")
        for name, our_tensor in our.items():
            self.assertTrue(our_tensor.is_contiguous())
            self._compare_tensors_robustly(our_tensor, their[name], name)

    def test_truncated_header(self):
        """Tests that truncation within the JSON header raises an error."""
        file_path = create_random_safetensors(self.temp_dir)
        
        # Read the header length to know where the header ends
        with open(file_path, "rb") as f:
            header_size_bytes = f.read(8)
            header_size = int.from_bytes(header_size_bytes, 'little')
        
        # Truncate to just 4 bytes into the header (mid-JSON)
        # File = 8 bytes (size) + header_size + data
        truncated_size = 8 + (header_size // 2)
        
        with open(file_path, "r+b") as f:
            f.truncate(truncated_size)
        
        with SafetensorsStreamer() as run_sf:
            # Should fail during header parsing/JSON decoding
            with self.assertRaises(ValueError):
                run_sf.stream_file(file_path, None, "cpu", False)
                for _ in run_sf.get_tensors():
                    pass

    def test_truncated_data(self):
        """Tests that truncation within the data section raises an error."""
        file_path = create_random_safetensors(self.temp_dir)
        
        with open(file_path, "rb") as f:
            header_size = int.from_bytes(f.read(8), 'little')
        
        # Header is fully intact, but data is cut short.
        # We truncate at Header + 8 (size prefix) + a few bytes of data
        header_end_pos = 8 + header_size
        original_size = os.path.getsize(file_path)
        
        # Truncate halfway through the actual tensor data
        truncated_size = header_end_pos + ((original_size - header_end_pos) // 2)
        
        with open(file_path, "r+b") as f:
            f.truncate(truncated_size)
        
        with SafetensorsStreamer() as run_sf:
            # Header parsing might pass, but reading tensor buffers must fail
            with self.assertRaises(ValueError):
                run_sf.stream_file(file_path, None, "cpu", False)
                for _ in run_sf.get_tensors():
                    pass

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

if __name__ == "__main__":
    unittest.main()