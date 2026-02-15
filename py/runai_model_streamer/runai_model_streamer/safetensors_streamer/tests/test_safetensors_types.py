from __future__ import annotations
import unittest
import torch
import numpy as np
import runai_model_streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch

class TestSafetensorsDtypes(unittest.TestCase):

    def setUp(self):
        """Reference the live mapping from your streamer package."""
        self.dtype_map = safetensors_pytorch.safetenors_to_torch_dtype

    def validate_logic(self, dtype_str: str, shape: list[int], actual_bytes: int):
        """
        Replicates the revised _validate_shape_consistency logic using 
        bit-width math to handle sub-byte packing accurately.
        """
        num_elements = 1
        for dim in shape:
            num_elements *= dim
            
        if dtype_str not in self.dtype_map:
            raise KeyError(f"Dtype {dtype_str} missing from safetensors_pytorch.py")
            
        # Define bit-widths for sub-byte types aligned with our final streamer logic
        # We only keep what's in our official dictionary
        bit_widths = {
            "F4": 4,
            "F6_E3M2": 6,
            "F6_E2M3": 6
        }

        if dtype_str in bit_widths:
            bits_per_element = bit_widths[dtype_str]
            # Ceiling division for total bytes: (Total Bits + 7) // 8
            expected_bytes = (num_elements * bits_per_element + 7) // 8
        else:
            # Standard byte-aligned types (8, 16, 32, 64-bit)
            torch_dtype = self.dtype_map[dtype_str]
            element_size = torch.tensor([], dtype=torch_dtype).element_size()
            expected_bytes = num_elements * element_size

        # Fallback: Check for 'Lazy Exporters' who use 1-byte containers 
        # for sub-byte types (not packing them)
        if dtype_str in bit_widths and actual_bytes == num_elements:
            return True

        return expected_bytes == actual_bytes

    def test_all_mapped_dtypes_exist(self):
        """Verify only the official Safetensors strings are present."""
        # Removed I4 and F4_E1M1 as they aren't in our finalized official list
        # Keeping F4 as the primary 4-bit representative
        required_types = ["F8_E8M0", "F6_E3M2", "F4"]
        for t in required_types:
            with self.subTest(dtype=t):
                self.assertIn(t, self.dtype_map, f"Crucial type {t} is missing from safetensors_pytorch.py")

    def test_packed_4bit_validation(self):
        """Tests bit-width logic for the official F4 packed format."""
        # 512 elements @ 4-bits = 2048 bits = 256 bytes
        # This matches the (bits + 7) // 8 logic in the streamer
        self.assertTrue(self.validate_logic("F4", [512], 256))
        
        # Test odd shape: 9 elements @ 4-bits = 36 bits = 5 bytes (rounded up)
        # This is a critical edge case for GPT-OSS-120B padding
        self.assertTrue(self.validate_logic("F4", [3, 3], 5))

    def test_mx_scale_validation(self):
        """Tests the F8_E8M0 (OCP Microscaling) 8-bit scale type."""
        # 128 elements in F8_E8M0 should be exactly 128 bytes.
        self.assertTrue(self.validate_logic("F8_E8M0", [128], 128))

    def test_float6_validation(self):
        """Tests 6-bit floats stored in 1-byte containers."""
        # F6 formats are typically byte-aligned in safetensors.
        self.assertTrue(self.validate_logic("F6_E3M2", [64, 1], 64))

    def test_copyless_frombuffer_all_types(self):
        """
        Verifies torch.frombuffer correctly creates a view for every single 
        dtype defined in your dictionary.
        """
        raw_bytes = bytearray([0] * 8) # 8 bytes can hold at least one element of any type
        buffer = memoryview(raw_bytes)

        for dtype_str, torch_dtype in self.dtype_map.items():
            with self.subTest(dtype=dtype_str):
                try:
                    # Create tensor from buffer
                    tensor = torch.frombuffer(buffer, dtype=torch_dtype)
                    
                    # Verify it's copyless: Change original byte
                    raw_bytes[0] = 123
                    
                    # For multi-byte types (F64, I32), we check the first byte-equivalent
                    # Converting to uint8 view for comparison is the most reliable check
                    tensor_as_bytes = tensor.view(torch.uint8)
                    self.assertEqual(tensor_as_bytes[0].item(), 123, f"Copy detected for {dtype_str}!")
                except Exception as e:
                    self.fail(f"Failed to create copyless tensor for {dtype_str}: {e}")

if __name__ == "__main__":
    unittest.main()