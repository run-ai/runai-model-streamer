import os
import torch
import string
import random
import json

# --- Constants ---
MIN_NUM_FILES, MAX_NUM_FILES = 1, 3
MIN_NUM_TENSORS, MAX_NUM_TENSORS = 1, 10
MAX_NUM_TENSORS_SMALL = 5
MIN_TENSOR_NAME_LEN, MAX_TENSOR_NAME_LEN = 5, 20

TYPE_TO_STR = {
    torch.float64: "F64", torch.float32: "F32", torch.float16: "F16", torch.bfloat16: "BF16",
    torch.int64: "I64", torch.int32: "I32", torch.int16: "I16", torch.int8: "I8",
    torch.uint8: "U8", torch.bool: "BOOL",
}

EXPERIMENTAL_STR_MAP = {
    "float8_e4m3fn": "F8_E4M3",
    "float8_e5m2": "F8_E5M2",
    "float8_e8m0fnu": "F8_E8M0",
}

def get_dtype_str(dtype):
    if dtype in TYPE_TO_STR:
        return TYPE_TO_STR[dtype]
    d_str = str(dtype).split('.')[-1]
    return EXPERIMENTAL_STR_MAP.get(d_str, "U8")

def random_name():
    characters = string.ascii_letters + string.digits + "_"
    return "tensor_" + "".join(random.choice(characters) for _ in range(random.randint(5, 15)))

def generate_random_data(min_tensors, max_tensors):
    all_available = list(TYPE_TO_STR.keys())
    for name in EXPERIMENTAL_STR_MAP.keys():
        if hasattr(torch, name):
            all_available.append(getattr(torch, name))

    tensors = {}
    for _ in range(random.randint(min_tensors, max_tensors)):
        dtype = random.choice(all_available)
        shape = tuple(random.randint(1, 10) for _ in range(random.randint(1, 4)))
        
        if dtype == torch.bool:
            t = torch.rand(shape) > 0.5
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
    save_manual_safetensors(generate_random_data(MIN_NUM_TENSORS, MAX_NUM_TENSORS), path)
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
        save_manual_safetensors(data, file_path)
        file_paths.append(file_path)
        
    return file_paths