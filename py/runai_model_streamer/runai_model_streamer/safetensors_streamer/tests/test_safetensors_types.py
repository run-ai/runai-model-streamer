from __future__ import annotations
import unittest
import torch
import numpy as np
# Assuming the package structure exists
import runai_model_streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch

class TestSafetensorsDtypes(unittest.TestCase):

    def setUp(self):
        """Reference the live mapping from your streamer package."""
        self.dtype_map = safetensors_pytorch.safetensors_to_torch_dtype

    def validate_logic(self, dtype_str: str, shape: list[int], actual_bytes: int):
        """
        Calculates expected byte size and compares with actual buffer size.
        Handles both byte-aligned (F8, F16) and packed (F4) types.
        """
        num_elements = 1
        for dim in shape:
            num_elements *= dim
            
        # Define bit-widths for types that are NOT 1-byte aligned
        # If a type is 8-bit or larger, we use torch.element_size()
        bit_widths = {
            "F4": 4, 
        }

        if dtype_str in bit_widths:
            bits_per_element = bit_widths[dtype_str]
            # Ceiling division: (Total Bits + 7) // 8
            expected_bytes = (num_elements * bits_per_element + 7) // 8
        else:
            # Standard path for F8, F16, F32, etc.
            if dtype_str not in self.dtype_map:
                raise KeyError(f"Dtype {dtype_str} missing from dtype_map")
            
            torch_dtype = self.dtype_map[dtype_str]
            # Handle cases where experimental dtypes might be None or custom objects
            try:
                element_size = torch.tensor([], dtype=torch_dtype).element_size()
            except:
                element_size = 1 # Fallback for 8-bit experimental types
            expected_bytes = num_elements * element_size

        return expected_bytes == actual_bytes

    def test_all_mapped_dtypes_exist(self):
        """Verify crucial industry-standard experimental types are present."""
        required_types = ["F8_E4M3", "F8_E5M2", "F8_E8M0"]
        for t in required_types:
            with self.subTest(dtype=t):
                self.assertIn(t, self.dtype_map, f"Crucial type {t} is missing from mapping!")

    def test_float8_validation(self):
        """Tests standard 1-byte alignment for F8 variants (E4M3, E5M2, E8M0)."""
        # 1024 elements should be exactly 1024 bytes
        for f8_type in ["F8_E4M3", "F8_E5M2", "F8_E8M0"]:
            with self.subTest(dtype=f8_type):
                self.assertTrue(self.validate_logic(f8_type, [1024], 1024))

    def test_copyless_frombuffer_safe(self):
        """
        Verifies torch.frombuffer handles all types without crashing.
        Sub-byte types are viewed as uint8 to prevent PyTorch DType errors.
        """
        raw_bytes = bytearray([0] * 8) 
        buffer = memoryview(raw_bytes)

        for dtype_str, torch_dtype in self.dtype_map.items():
            # Skip if the torch version doesn't support the experimental dtype yet
            if torch_dtype is None:
                continue

            with self.subTest(dtype=dtype_str):
                try:
                    # Logic: If it's a sub-byte type (F4), we MUST load as uint8
                    # because PyTorch cannot create a 'torch.float4' tensor view.
                    load_dtype = torch_dtype if "F4" not in dtype_str else torch.uint8
                    
                    tensor = torch.frombuffer(buffer, dtype=load_dtype)
                    
                    # Verify it's a zero-copy view
                    raw_bytes[0] = 42
                    tensor_as_bytes = tensor.view(torch.uint8)
                    self.assertEqual(tensor_as_bytes[0].item(), 42, f"Copy detected for {dtype_str}")
                except Exception as e:
                    self.fail(f"Copyless check failed for {dtype_str}: {e}")

    def test_shape_mismatch_detection(self):
        """Ensure the validator fails when byte size doesn't match shape."""
        # F16: 10 elements should be 20 bytes. Providing 15 should fail.
        self.assertFalse(self.validate_logic("F16", [10], 15))

if __name__ == "__main__":
    unittest.main()