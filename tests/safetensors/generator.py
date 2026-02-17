import os
import torch
import string
import random
import json
from safetensors.torch import save_file

# --- Constants ---
MIN_NUM_FILES, MAX_NUM_FILES = 1, 3
MIN_NUM_TENSORS, MAX_NUM_TENSORS = 1, 10
MAX_NUM_TENSORS_SMALL = 5
MIN_TENSOR_NAME_LEN, MAX_TENSOR_NAME_LEN = 5, 20

def _can_safetensors_save(dtype):
    """Check if the current safetensors version can save this dtype."""
    import tempfile
    try:
        with tempfile.NamedTemporaryFile(suffix=".safetensors", delete=True) as f:
            test_tensor = torch.zeros((1,), dtype=dtype)
            save_file({"test": test_tensor}, f.name)
        return True
    except (KeyError, ValueError):
        return False

# All types our streamer supports (for reading files)
TYPE_TO_STR = {
    torch.float64: "F64", torch.float32: "F32", torch.float16: "F16", torch.bfloat16: "BF16",
    torch.int64: "I64", torch.int32: "I32", torch.int16: "I16", torch.int8: "I8",
    torch.uint8: "U8", torch.bool: "BOOL",
    torch.complex64: "C64",
}

# Add unsigned types if available (PyTorch >= 2.3.0)
if hasattr(torch, "uint16"):
    TYPE_TO_STR[torch.uint16] = "U16"
if hasattr(torch, "uint32"):
    TYPE_TO_STR[torch.uint32] = "U32"
if hasattr(torch, "uint64"):
    TYPE_TO_STR[torch.uint64] = "U64"

# Add experimental float8 types if available (PyTorch >= 2.1)
if hasattr(torch, "float8_e4m3fn"):
    TYPE_TO_STR[torch.float8_e4m3fn] = "F8_E4M3"
if hasattr(torch, "float8_e5m2"):
    TYPE_TO_STR[torch.float8_e5m2] = "F8_E5M2"
if hasattr(torch, "float8_e8m0fnu"):
    TYPE_TO_STR[torch.float8_e8m0fnu] = "F8_E8M0"

# Dynamically detect which types the current safetensors version can save
# This tests each type by attempting to save it with safetensors
SAFETENSORS_SUPPORTED_TYPES = {}
for dtype, dtype_str in TYPE_TO_STR.items():
    if _can_safetensors_save(dtype):
        SAFETENSORS_SUPPORTED_TYPES[dtype] = dtype_str

def get_dtype_str(dtype):
    """
    Convert a PyTorch dtype to its safetensors string representation.

    Raises ValueError if dtype is not in TYPE_TO_STR, indicating a bug in type detection.
    """
    if dtype in TYPE_TO_STR:
        return TYPE_TO_STR[dtype]

    # If we reach here, it means a dtype was used that wasn't properly registered
    # This indicates a bug in the type detection logic
    raise ValueError(
        f"Unsupported dtype {dtype}. This dtype should have been added to TYPE_TO_STR "
        f"or filtered out by SAFETENSORS_SUPPORTED_TYPES. This is a bug in generator.py."
    )

def random_name():
    characters = string.ascii_letters + string.digits + "_"
    return "tensor_" + "".join(random.choice(characters) for _ in range(random.randint(5, 15)))

def generate_random_data(min_tensors, max_tensors):
    """
    Generate random tensor data using all types supported by both PyTorch and safetensors.

    SAFETENSORS_SUPPORTED_TYPES already contains only the types that:
    1. Are available in the current PyTorch version
    2. Can be saved by the current safetensors version
    """
    all_available = list(SAFETENSORS_SUPPORTED_TYPES.keys())

    if not all_available:
        raise RuntimeError(
            "No supported types detected! This should never happen. "
            "Check that PyTorch and safetensors are properly installed."
        )

    tensors = {}
    for _ in range(random.randint(min_tensors, max_tensors)):
        dtype = random.choice(all_available)
        shape = tuple(random.randint(1, 10) for _ in range(random.randint(1, 4)))

        if dtype == torch.bool:
            t = torch.rand(shape) > 0.5
        elif dtype.is_complex:
            # Generate complex tensors with random real and imaginary parts
            real = torch.rand(shape, device="cpu") - 0.5
            imag = torch.rand(shape, device="cpu") - 0.5
            t = torch.complex(real, imag).to(dtype)
        elif dtype.is_floating_point:
            # Shift range to [-0.5, 0.5) to test the Sign Bit (Proper Bit Binding)
            t = (torch.rand(shape, device="cpu") - 0.5).to(dtype)
        else:
            t = torch.randint(0, 100, shape, dtype=dtype)

        tensors[random_name()] = t
    return tensors

def save_manual_safetensors(tensors_dict, path):
    header = {}
    current_offset = 0
    buffer_parts = []

    for name, tensor in tensors_dict.items():
        t = tensor.detach().cpu().contiguous()
        # View as uint8 for raw byte extraction
        raw_data = t.view(torch.uint8).numpy().tobytes()
        
        length = len(raw_data)
        header[name] = {
            "dtype": get_dtype_str(tensor.dtype),
            "shape": list(tensor.shape),
            "data_offsets": [current_offset, current_offset + length]
        }
        buffer_parts.append(raw_data)
        current_offset += length

    header["__metadata__"] = {"generated_by": "manual_bypass_generator"}
    header_bytes = json.dumps(header).encode('utf-8')
    padding = (8 - len(header_bytes) % 8) % 8
    header_bytes += b' ' * padding
    
    with open(path, "wb") as f:
        f.write(len(header_bytes).to_bytes(8, 'little'))
        f.write(header_bytes)
        for part in buffer_parts:
            f.write(part)

def create_random_safetensors(dir_path):
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, "model.safetensors")
    tensors = generate_random_data(MIN_NUM_TENSORS, MAX_NUM_TENSORS)
    # Use official library to ensure compatibility
    save_file(tensors, path)
    return path

def create_random_multi_safetensors(dir_path):
    """Generates multiple safetensors files to simulate a sharded model."""
    os.makedirs(dir_path, exist_ok=True)
    file_paths = []
    num_files = random.randint(MIN_NUM_FILES, MAX_NUM_FILES)

    for i in range(num_files):
        # Using the _SMALL constant to keep the total payload manageable
        file_path = os.path.join(dir_path, f"model-{i:05d}-of-{num_files:05d}.safetensors")
        data = generate_random_data(MIN_NUM_TENSORS, MAX_NUM_TENSORS_SMALL)
        # Use official library to ensure compatibility
        save_file(data, file_path)
        file_paths.append(file_path)

    return file_paths
