# This file is used to generate random safetensors files for testing purposes.
import os
import torch
import string
import random
import json
import numpy as np

# --- Constants ---
MIN_NUM_FILES, MAX_NUM_FILES = 1, 3
MIN_NUM_TENSORS, MAX_NUM_TENSORS = 1, 10
MAX_NUM_TENSORS_SMALL = 5  # Fixed: Added missing constant
MIN_TENSOR_NAME_LEN, MAX_TENSOR_NAME_LEN = 5, 20
MIN_TENSOR_SHAPE_DIM, MAX_TENSOR_SHAPE_DIM = 1, 4

# Mapping torch dtypes to the string names we want in the Safetensors header
TYPE_TO_STR = {
    torch.float64: "F64", torch.float32: "F32", torch.float16: "F16", torch.bfloat16: "BF16",
    torch.int64: "I64", torch.int32: "I32", torch.int16: "I16", torch.int8: "I8",
    torch.uint8: "U8", torch.bool: "BOOL",
}

EXPERIMENTAL_STR_MAP = {
    "float8_e4m3fn": "F8_E4M3",        # Matched to Rust
    "float8_e5m2": "F8_E5M2",          # Matched to Rust
    "float8_e8m0fnu": "F8_E8M0",       # Matched to Rust
    "float4_e2m1fn_x2": "F4",          # Matched to Rust F4
    "float8_e4m3fnuz": "F8_E4M3",      # Fallback to base F8
    "float8_e5m2fnuz": "F8_E5M2",      # Fallback to base F8
}

def get_dtype_str(dtype):
    if dtype in TYPE_TO_STR:
        return TYPE_TO_STR[dtype]
    d_str = str(dtype).split('.')[-1]
    return EXPERIMENTAL_STR_MAP.get(d_str, "U8")

def random_name():
    characters = string.ascii_letters + string.digits + "_"
    return "tensor_" + "".join(random.choice(characters) for _ in range(random.randint(5, 15)))

def save_manual_safetensors(tensors_dict, path):
    header = {}
    current_offset = 0
    buffer_parts = []

    for name, tensor in tensors_dict.items():
        # Ensure CPU and contiguous
        t = tensor.detach().cpu().contiguous()
        # View as uint8 to bypass NumPy/ScalarType validation errors
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

def generate_random_data(min_tensors, max_tensors):
    all_available = list(TYPE_TO_STR.keys())
    for name in EXPERIMENTAL_STR_MAP.keys():
        if hasattr(torch, name):
            all_available.append(getattr(torch, name))

    tensors = {}
    for _ in range(random.randint(min_tensors, max_tensors)):
        dtype = random.choice(all_available)
        shape = tuple(random.randint(1, 10) for _ in range(random.randint(1, 4)))
        
        # Fixed: Branching logic for ALL non-float types
        if dtype == torch.bool:
            t = torch.rand(shape) > 0.5
        elif dtype in [torch.int64, torch.int32, torch.int16, torch.int8, torch.uint8]:
            t = torch.randint(0, 100, shape, dtype=dtype)
        elif any(x in str(dtype) for x in ['float8', 'float4']):
            # Experimental types: create via uint8 bits
            t = torch.randint(0, 255, shape, dtype=torch.uint8).view(dtype)
        else:
            # Standard floats (F32, F16, BF16, F64)
            t = torch.rand(shape, dtype=dtype)
        
        tensors[random_name()] = t
    return tensors

def create_random_safetensors(dir_path):
    os.makedirs(dir_path, exist_ok=True)
    path = os.path.join(dir_path, "model.safetensors")
    save_manual_safetensors(generate_random_data(MIN_NUM_TENSORS, MAX_NUM_TENSORS), path)
    return path

def create_random_multi_safetensors(dir_path):
    os.makedirs(dir_path, exist_ok=True)
    file_paths = []
    for i in range(random.randint(MIN_NUM_FILES, MAX_NUM_FILES)):
        file_path = os.path.join(dir_path, f"model-{i}.safetensors")
        save_manual_safetensors(generate_random_data(MIN_NUM_TENSORS, MAX_NUM_TENSORS_SMALL), file_path)
        file_paths.append(file_path)
    return file_paths