from streamer import (dll, t_streamer, t_streamer_p)
from typing import List
import ctypes


def runai_start(streamer: t_streamer_p) -> int:
    return dll.fn_runai_start(streamer)


def runai_end(streamer: t_streamer) -> None:
    return dll.fn_runai_end(streamer)


def runai_request(
    streamer: t_streamer,
    path: str,
    offset: int,
    bytesize: int,
    dst: memoryview,
    num_sizes: int,
    internal_sizes: List[int],
) -> int:
    return dll.fn_runai_request(
        streamer,
        path.encode("utf-8"),
        offset,
        bytesize,
        dst,
        num_sizes,
        (ctypes.c_uint64 * num_sizes)(*internal_sizes),
    )


def runai_response(streamer: t_streamer, index: List[int]) -> int:
    value = ctypes.c_uint32()
    result = dll.fn_runai_response(streamer, ctypes.byref(value))
    index[0] = value.value
    return result


def runai_response_str(response_code: int) -> str:
    return dll.fn_runai_response_str(response_code)
