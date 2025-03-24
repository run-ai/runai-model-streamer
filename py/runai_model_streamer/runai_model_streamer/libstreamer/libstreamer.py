from runai_model_streamer.libstreamer import dll, t_streamer
from typing import List, Optional
import ctypes

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
)

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
    s3_credentials: Optional[S3Credentials] = None,
) -> None:
    c_dst = (ctypes.c_ubyte * len(dst)).from_buffer(dst)
    error_code = dll.fn_runai_read_with_credentials(
        streamer,
        path.encode("utf-8"),
        offset,
        bytesize,
        c_dst,
        ctypes.c_char_p(s3_credentials.access_key_id.encode("utf-8")) if s3_credentials is not None and s3_credentials.access_key_id is not None else None,
        ctypes.c_char_p(s3_credentials.secret_access_key.encode("utf-8")) if s3_credentials is not None and s3_credentials.secret_access_key is not None else None,
        ctypes.c_char_p(s3_credentials.session_token.encode("utf-8")) if s3_credentials is not None and s3_credentials.session_token is not None else None,
        ctypes.c_char_p(s3_credentials.region_name.encode("utf-8")) if s3_credentials is not None and s3_credentials.region_name is not None else None,
        ctypes.c_char_p(s3_credentials.endpoint.encode("utf-8")) if s3_credentials is not None and s3_credentials.endpoint is not None else None,
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
    s3_credentials: Optional[S3Credentials] = None,
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
        ctypes.c_char_p(s3_credentials.access_key_id.encode("utf-8")) if s3_credentials is not None and s3_credentials.access_key_id is not None else None,
        ctypes.c_char_p(s3_credentials.secret_access_key.encode("utf-8")) if s3_credentials is not None and s3_credentials.secret_access_key is not None else None,
        ctypes.c_char_p(s3_credentials.session_token.encode("utf-8")) if s3_credentials is not None and s3_credentials.session_token is not None else None,
        ctypes.c_char_p(s3_credentials.region_name.encode("utf-8")) if s3_credentials is not None and s3_credentials.region_name is not None else None,
        ctypes.c_char_p(s3_credentials.endpoint.encode("utf-8")) if s3_credentials is not None and s3_credentials.endpoint is not None else None,
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
