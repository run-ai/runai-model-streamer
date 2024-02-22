from streamer import dll
from typing import List
import ctypes

def runai_start(size: int, chunk_bytesize: int, block_size: int) -> int:
    return dll.fn_runai_start(size, chunk_bytesize, block_size)

def runai_end() -> None:
    return dll.fn_runai_end()

def runai_request(path: str, offset: int, bytesize: int, dst: memoryview, num_sizes: int, internal_sizes: List[int]) -> int:
    return dll.fn_runai_request(path.encode('utf-8'), offset, bytesize, dst, num_sizes, (ctypes.c_uint64 * num_sizes)(*internal_sizes))

def runai_response() -> int:
    return dll.fn_runai_response()