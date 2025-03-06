from runai_model_streamer.libstreamer import dll, t_streamer
from typing import List, Optional
import ctypes

SUCCESS_ERROR_CODE = 0
FINISHED_ERROR_CODE = 1


def runai_start() -> t_streamer:
    streamer = t_streamer(0)
    error_code = dll.fn_runai_start(ctypes.byref(streamer))
    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not open streamer in libstreamer due to: {runai_response_str(error_code)}"
        )
    return streamer


def runai_end(streamer: t_streamer) -> None:
    return dll.fn_runai_end(streamer)


def runai_read(
    streamer: t_streamer,
    path: str,
    offset: int,
    bytesize: int,
    dst: memoryview,
) -> None:
    c_dst = (ctypes.c_ubyte * len(dst)).from_buffer(dst)
    error_code = dll.fn_runai_read(
        streamer,
        path.encode("utf-8"),
        offset,
        bytesize,
        c_dst,
    )
    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not send runai_request to libstreamer due to: {runai_response_str(error_code)}"
        )
    
def runai_read_with_credentials(
    streamer: t_streamer,
    path: str,
    offset: int,
    bytesize: int,
    dst: memoryview,
    access_id: Optional[str] = None,
    access_key: Optional[str] = None,
    access_token: Optional[str] = None,
    region: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> None:
    c_dst = (ctypes.c_ubyte * len(dst)).from_buffer(dst)
    error_code = dll.fn_runai_read_with_credentials(
        streamer,
        path.encode("utf-8"),
        offset,
        bytesize,
        c_dst,
        ctypes.c_char_p(access_id.encode("utf-8")) if access_id is not None else None,
        ctypes.c_char_p(access_key.encode("utf-8")) if access_key is not None else None,
        ctypes.c_char_p(access_token.encode("utf-8")) if access_token is not None else None,
        ctypes.c_char_p(region.encode("utf-8")) if region is not None else None,
        ctypes.c_char_p(endpoint.encode("utf-8")) if endpoint is not None else None,
    )
    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not send runai_request_with_credentials to libstreamer due to: {runai_response_str(error_code)}"
        )


def runai_request(
    streamer: t_streamer,
    path: str,
    offset: int,
    bytesize: int,
    dst: memoryview,
    internal_sizes: List[int],
) -> None:
    c_dst = (ctypes.c_ubyte * len(dst)).from_buffer(dst)
    error_code = dll.fn_runai_request(
        streamer,
        path.encode("utf-8"),
        offset,
        bytesize,
        c_dst,
        len(internal_sizes),
        (ctypes.c_uint64 * len(internal_sizes))(*internal_sizes),
    )
    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not send runai_request to libstreamer due to: {runai_response_str(error_code)}"
        )

def runai_request_with_credentials(
    streamer: t_streamer,
    path: str,
    offset: int,
    bytesize: int,
    dst: memoryview,
    internal_sizes: List[int],
    access_id: Optional[str] = None,
    access_key: Optional[str] = None,
    access_token: Optional[str] = None,
    region: Optional[str] = None,
    endpoint: Optional[str] = None,
) -> None:
    c_dst = (ctypes.c_ubyte * len(dst)).from_buffer(dst)
    error_code = dll.fn_runai_request_with_credentials(
        streamer,
        path.encode("utf-8"),
        offset,
        bytesize,
        c_dst,
        len(internal_sizes),
        (ctypes.c_uint64 * len(internal_sizes))(*internal_sizes),
        ctypes.c_char_p(access_id.encode("utf-8")) if access_id is not None else None,
        ctypes.c_char_p(access_key.encode("utf-8")) if access_key is not None else None,
        ctypes.c_char_p(access_token.encode("utf-8")) if access_token is not None else None,
        ctypes.c_char_p(region.encode("utf-8")) if region is not None else None,
        ctypes.c_char_p(endpoint.encode("utf-8")) if endpoint is not None else None,
    )
    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not send runai_request_with_credentials to libstreamer due to: {runai_response_str(error_code)}"
        )


def runai_response(streamer: t_streamer) -> Optional[int]:
    value = ctypes.c_uint32()
    error_code = dll.fn_runai_response(streamer, ctypes.byref(value))
    if error_code == FINISHED_ERROR_CODE:
        return None

    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not receive runai_response from libstreamer due to: {runai_response_str(error_code)}"
        )
    return value.value


def runai_response_str(response_code: int) -> str:
    return dll.fn_runai_response_str(response_code)
