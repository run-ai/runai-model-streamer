from runai_model_streamer.libstreamer import dll, t_streamer
from typing import List, Optional, Tuple
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

def runai_request(
    streamer: t_streamer,
    paths: List[str],
    file_offsets: List[int],
    bytesizes: List[int],
    dsts: List[memoryview],
    internal_sizes: List[List[int]],
    s3_credentials: Optional[S3Credentials] = None,
) -> None:
    c_paths = (ctypes.c_char_p * len(paths))(*[path.encode("utf-8") for path in paths])
    c_file_offsets = (ctypes.c_uint64 * len(file_offsets))(*file_offsets)
    c_bytesizes = (ctypes.c_uint64 * len(bytesizes))(*bytesizes)
    dst_addrs = [
        ctypes.cast(ctypes.c_void_p(ctypes.addressof(ctypes.c_char.from_buffer(dst))), ctypes.c_void_p)
        for dst in dsts
    ]
    c_dsts = (ctypes.c_void_p * len(dst_addrs))(*dst_addrs)
    
    num_files = len(paths)

    # c_num_sizes: An array where each element is the number of ranges for the corresponding file.
    # Assuming the number of ranges fits into a c_uint32.
    num_ranges_per_file_list = [len(sublist) for sublist in internal_sizes]
    c_num_sizes = (ctypes.c_uint32 * num_files)(*num_ranges_per_file_list)

    # c_internal_sizes: An array of pointers, where each pointer points to an array of actual range sizes.

    # Create and store the actual ctypes arrays of range sizes.
    _c_internal_sizes_data_arrays = [
        (ctypes.c_uint64 * num_ranges_for_this_file)(*actual_sublist_data)
        for num_ranges_for_this_file, actual_sublist_data in zip(num_ranges_per_file_list, internal_sizes)
    ]

    # Create the array of pointers that the C function expects.
    PtrToUint64ArrayType = ctypes.POINTER(ctypes.c_uint64)
    c_internal_sizes = (PtrToUint64ArrayType * num_files)()

    # Populate this array of pointers.
    for i, individual_c_array_obj in enumerate(_c_internal_sizes_data_arrays):
        c_internal_sizes[i] = ctypes.cast(individual_c_array_obj, PtrToUint64ArrayType)
    
    error_code = dll.fn_runai_request(
        streamer,
        len(paths),
        c_paths,
        c_file_offsets,
        c_bytesizes,
        c_dsts, 
        c_num_sizes,
        c_internal_sizes,
        ctypes.c_char_p(s3_credentials.access_key_id.encode("utf-8")) if s3_credentials is not None and s3_credentials.access_key_id is not None else None,
        ctypes.c_char_p(s3_credentials.secret_access_key.encode("utf-8")) if s3_credentials is not None and s3_credentials.secret_access_key is not None else None,
        ctypes.c_char_p(s3_credentials.session_token.encode("utf-8")) if s3_credentials is not None and s3_credentials.session_token is not None else None,
        ctypes.c_char_p(s3_credentials.region_name.encode("utf-8")) if s3_credentials is not None and s3_credentials.region_name is not None else None,
        ctypes.c_char_p(s3_credentials.endpoint.encode("utf-8")) if s3_credentials is not None and s3_credentials.endpoint is not None else None,   
    )
    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not send runai_request to libstreamer due to: {runai_response_str(error_code)}"
        )

def runai_response(streamer: t_streamer) -> Optional[Tuple[int, int]]:
    file_index = ctypes.c_uint32()
    range_index = ctypes.c_uint32()
    error_code = dll.fn_runai_response(streamer, ctypes.byref(file_index), ctypes.byref(range_index))
    if error_code == FINISHED_ERROR_CODE:
        return None
    if error_code != SUCCESS_ERROR_CODE:
        raise Exception(
            f"Could not receive runai_response from libstreamer due to: {runai_response_str(error_code)}"
        )
    return file_index.value, range_index.value

def runai_response_str(response_code: int) -> str:
    return dll.fn_runai_response_str(response_code)
