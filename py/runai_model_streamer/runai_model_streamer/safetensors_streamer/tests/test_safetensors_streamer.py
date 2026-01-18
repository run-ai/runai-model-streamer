import unittest
import torch
import os
import struct
import json
import tempfile
import shutil
from safetensors import safe_open
from runai_model_streamer.safetensors_streamer.safetensors_streamer import (
    SafetensorsStreamer,
)

# Constants for binary generation
HEADER_SIZE_FORMAT = "<Q"  # Little-endian unsigned long long (8 bytes)

class TestSafetensorsStreamer(unittest.TestCase):

    def setUp(self):
        # Create a temporary directory for generating corrupted files
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Cleanup after tests
        shutil.rmtree(self.test_dir)

    def create_corrupted_safetensors(self, filename, header_len_val, header_content, tensor_data=b""):
        """
        Helper to craft raw safetensors files byte-by-byte.
        
        Args:
            header_len_val: The integer value to put in the first 8 bytes.
            header_content: String or bytes to put in the header body.
            tensor_data: Bytes to append after the header.
        """
        filepath = os.path.join(self.test_dir, filename)
        
        # Ensure header content is bytes
        if isinstance(header_content, str):
            header_content = header_content.encode('utf-8')
            
        with open(filepath, "wb") as f:
            # 1. Write the 8-byte length
            f.write(struct.pack(HEADER_SIZE_FORMAT, header_len_val))
            
            # 2. Write the JSON (or garbage) header
            f.write(header_content)
            
            # 3. Write the tensor data
            f.write(tensor_data)
        
        return filepath

    def test_valid_file(self):
        # Assuming test_files exists relative to the script location
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "test_files", "test.safetensors")
        
        if not os.path.exists(file_path):
            self.skipTest(f"Original test file not found at {file_path}")

        our = {}
        with SafetensorsStreamer() as run_sf:
            run_sf.stream_file(file_path, None, "cpu")
            for name, tensor in run_sf.get_tensors():
                our[name] = tensor

        their = {}
        with safe_open(file_path, framework="pt", device="cpu") as f:
            for name in f.keys():
                their[name] = f.get_tensor(name)

        self.assertEqual(len(our), len(their))
        for name, our_tensor in our.items():
            self.assertTrue(our_tensor.is_contiguous())
            self.assertEqual(our_tensor.dtype, their[name].dtype)
            self.assertEqual(our_tensor.shape, their[name].shape)
            self.assertTrue(torch.all(our_tensor.eq(their[name])))

    # -------------------------------------------------------------------------
    # CORRUPTION TESTS
    # -------------------------------------------------------------------------

    def test_header_too_large(self):
        """Test the '18 Exabyte' crash scenario (MAX_HEADER_SIZE check)."""
        # Create a file claiming its header is 101 MB (just over the 100MB limit)
        huge_size = (100 * 1024 * 1024) + 1
        path = self.create_corrupted_safetensors("too_large.st", huge_size, b"{}")

        with SafetensorsStreamer() as streamer:
            with self.assertRaisesRegex(ValueError, "exceeds limit"):
                streamer.stream_file(path, None, "cpu")

    def test_invalid_json(self):
        """Test catching broken JSON syntax."""
        # A header that cuts off before closing brace
        bad_json = '{"test": {"dtype": "F32", "shape": [1], "data_offsets": [0, 4]' 
        path = self.create_corrupted_safetensors("bad_json.st", len(bad_json), bad_json)

        with SafetensorsStreamer() as streamer:
            with self.assertRaisesRegex(ValueError, "not valid JSON"):
                streamer.stream_file(path, None, "cpu")

    def test_payload_inconsistency_shape_mismatch(self):
        """
        Test logic where Shape * Dtype size != Offset Length.
        This prevents reading garbage data or segfaults.
        """
        # Tensor claims to be F32 (4 bytes) * 10 elements = 40 bytes.
        # But offsets only reserve 4 bytes [0, 4].
        header_dict = {
            "test_tensor": {
                "dtype": "F32",
                "shape": [10],
                "data_offsets": [0, 4] 
            }
        }
        json_str = json.dumps(header_dict)
        # Provide 4 bytes of dummy data
        path = self.create_corrupted_safetensors("mismatch.st", len(json_str), json_str, b"\x00"*4)

        with SafetensorsStreamer() as streamer:
            # Expect the payload consistency check to fire
            with self.assertRaisesRegex(ValueError, "Shape claims 40 bytes.*but offsets reserve 4"):
                streamer.stream_file(path, None, "cpu")

    def test_overlapping_tensors(self):
        """Test logic ensuring tensors do not overlap in memory."""
        # Tensor A: [0, 10]
        # Tensor B: [5, 15] (Starts inside A)
        header_dict = {
            "A": {"dtype": "U8", "shape": [10], "data_offsets": [0, 10]},
            "B": {"dtype": "U8", "shape": [10], "data_offsets": [5, 15]}
        }
        json_str = json.dumps(header_dict)
        path = self.create_corrupted_safetensors("overlap.st", len(json_str), json_str, b"\x00"*20)

        with SafetensorsStreamer() as streamer:
            # Note: Dictionary ordering in python < 3.7 might make A or B come first.
            # Our code sorts by offset, so A (start=0) comes before B (start=5).
            # The error should trigger when checking B.
            with self.assertRaisesRegex(ValueError, "overlaps with next tensor"):
                streamer.stream_file(path, None, "cpu")

    def test_unknown_dtype(self):
        """Test handling of unsupported data types."""
        header_dict = {
            "test_tensor": {
                "dtype": "ALIEN_TYPE_128",
                "shape": [1],
                "data_offsets": [0, 4]
            }
        }
        json_str = json.dumps(header_dict)
        path = self.create_corrupted_safetensors("bad_dtype.st", len(json_str), json_str, b"\x00"*4)

        with SafetensorsStreamer() as streamer:
            with self.assertRaisesRegex(ValueError, "Unknown dtype"):
                streamer.stream_file(path, None, "cpu")

    def test_truncated_file_header(self):
        """Test a file that ends inside the header."""
        # Claim header is 100 bytes, but file only has 10 bytes total
        path = self.create_corrupted_safetensors("truncated.st", 100, b"short")
        
        # Depending on implementation, this might raise JSON error or Truncated error.
        # But it MUST raise something.
        with SafetensorsStreamer() as streamer:
            with self.assertRaises(ValueError):
                streamer.stream_file(path, None, "cpu")


if __name__ == "__main__":
    unittest.main()