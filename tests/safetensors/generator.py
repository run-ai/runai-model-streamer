import torch
import string
import random
from safetensors.torch import save_file

MIN_NUM_TENSORS = 1
MAX_NUM_TENSORS = 1024
MIN_TENSOR_SIZE = 16
MAX_TENSOR_SIZE = 16777216
MIN_TENSOR_NAME_LEN = 1
MAX_TENSOR_NAME_LEN = 248
MIN_TENSOR_SHAPE_DIM = 1
MAX_TENSOR_SHAPE_DIM = 10
ST_DATA_TYPES = [
    torch.float64,
    torch.float32,
    torch.float16,
    torch.bfloat16,
    torch.int64,
    torch.int32,
    torch.int16,
    torch.int8,
    torch.uint8,
    torch.bool,
]


def random_name():
    characters = string.ascii_letters + string.digits + "."
    return "".join(
        random.choice(characters)
        for _ in range(random.randint(MIN_TENSOR_NAME_LEN, MAX_TENSOR_NAME_LEN))
    )


def random_tensors():
    tensors = {}
    for i in range(random.randint(MIN_NUM_TENSORS, MAX_NUM_TENSORS)):
        dtype = random.shuffle(ST_DATA_TYPES)
        shape = tuple(
            random.randint(MIN_TENSOR_SHAPE_DIM, MAX_TENSOR_SHAPE_DIM)
            for _ in range(random.randint(MIN_TENSOR_SHAPE_DIM, MAX_TENSOR_SHAPE_DIM))
        )
        name = random_name()

        if dtype in [torch.int32, torch.int64]:
            tensor = torch.randint(low=0, high=100, size=shape, dtype=dtype)
        else:
            tensor = torch.rand(size=shape, dtype=dtype)

        tensors[name] = tensor
    return tensors


def create_random_safetensors(path):
    save_file(random_tensors(), path)