from runai_model_streamer.libstreamer import dll, t_streamer
from typing import Callable, Dict, List, Optional, Tuple
import ctypes

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
)

SUCCESS_ERROR_CODE = 0
FINISHED_ERROR_CODE = 1
# common::ResponseCode::TimedOut (see cpp/common/response_code/response_code.h)
TIMED_OUT_ERROR_CODE = 16


def _credentials_param_arrays(s3_credentials: Optional[S3Credentials]):
    # Marshal S3Credentials into the (param_keys, param_values, num_params) key/value form used by
    # runai_set_credentials. Only set fields are included; keys are the plugin's canonical config-param names.
    items = []
    if s3_credentials is not None:
        for key, value in (
            ("access_key_id", s3_credentials.access_key_id),
            ("secret_access_key", s3_credentials.secret_access_key),
            ("session_token", s3_credentials.session_token),
            ("region", s3_credentials.region_name),
            ("endpoint", s3_credentials.endpoint),
        ):
            if value is not None:
                items.append((key, value))
    if not items:
        return None, None, 0
    keys_arr = (ctypes.c_char_p * len(items))(*[k.encode("utf-8") for k, _ in items])
    values_arr = (ctypes.c_char_p * len(items))(*[v.encode("utf-8") for _, v in items])
    return keys_arr, values_arr, len(items)


def runai_set_credentials(
    streamer: t_streamer,
    s3_credentials: Optional[S3Credentials] = None,
) -> None:
    """Set the streamer's object-storage credentials (set-once, streamer-scoped). Safe to call again with the
    same credentials; a different set after the first raises."""
    keys_arr, values_arr, num_params = _credentials_param_arrays(s3_credentials)
    error_code = dll.fn_runai_set_credentials(streamer, keys_arr, values_arr, num_params)
    if error_code != SUCCESS_ERROR_CODE:
        raise ValueError(
            f"Could not set credentials in libstreamer due to: {runai_response_str(error_code)}"
        )

def runai_start() -> t_streamer:
    streamer = t_streamer(0)
    error_code = dll.fn_runai_start(ctypes.byref(streamer))
    if error_code != SUCCESS_ERROR_CODE:
        # Changed from Exception to ValueError
        raise ValueError(
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
) -> int:
    """Multi-request submit. Non-blocking: returns the assigned submission id; use it to demux the
    responses from runai_response. Credentials are streamer-scoped (runai_set_credentials), not passed
    here. Many submissions may be in flight at once."""
    num_files = len(paths)
    c_paths = (ctypes.c_char_p * num_files)(*[path.encode("utf-8") for path in paths])
    c_file_offsets = (ctypes.c_uint64 * len(file_offsets))(*file_offsets)
    c_bytesizes = (ctypes.c_uint64 * len(bytesizes))(*bytesizes)
    dst_addrs = [
        ctypes.cast(ctypes.c_void_p(ctypes.addressof(ctypes.c_char.from_buffer(dst))), ctypes.c_void_p)
        for dst in dsts
    ]
    c_dsts = (ctypes.c_void_p * len(dst_addrs))(*dst_addrs)

    # c_num_sizes: number of ranges for each file
    num_ranges_per_file_list = [len(sublist) for sublist in internal_sizes]
    c_num_sizes = (ctypes.c_uint32 * num_files)(*num_ranges_per_file_list)

    # c_internal_sizes: array of pointers, one per file, each to that file's array of range sizes. The backing
    # arrays are held in the local `internal_sizes_arrays` so they outlive the C call - c_internal_sizes stores
    # raw pointers into them, so they must not be garbage collected before fn_runai_request returns.
    internal_sizes_arrays = [
        (ctypes.c_uint64 * num_ranges_for_this_file)(*actual_sublist_data)
        for num_ranges_for_this_file, actual_sublist_data in zip(num_ranges_per_file_list, internal_sizes)
    ]
    PtrToUint64ArrayType = ctypes.POINTER(ctypes.c_uint64)
    c_internal_sizes = (PtrToUint64ArrayType * num_files)()
    for i, individual_c_array_obj in enumerate(internal_sizes_arrays):
        c_internal_sizes[i] = ctypes.cast(individual_c_array_obj, PtrToUint64ArrayType)

    submission_id = ctypes.c_uint64()
    error_code = dll.fn_runai_request(
        streamer,
        ctypes.byref(submission_id),
        num_files,
        c_paths,
        c_file_offsets,
        c_bytesizes,
        c_dsts,
        c_num_sizes,
        c_internal_sizes,
    )
    if error_code != SUCCESS_ERROR_CODE:
        raise ValueError(
            f"Could not send runai_request to libstreamer due to: {runai_response_str(error_code)}"
        )
    return submission_id.value


def runai_response(
    streamer: t_streamer,
    timeout_ms: int = 0,
) -> Optional[Tuple[int, int, int, int, bool]]:
    """Multi-request response. Returns (response_code, submission_id, file_index, index, submission_done) for
    the next ready sub-range of any in-flight submission. response_code is that sub-range's own result -
    Success (0) or a specific per-sub-range error - and the other fields are valid in BOTH cases, so a
    multi-submission caller can attribute an error to its submission and still learn submission_done (whether
    that submission is now complete). A per-sub-range error is NOT raised here: the caller decides what to do
    with it (e.g. abort only on UnknownError, or fail just the affected submission and keep draining others).
    Returns None on teardown (FinishedError). Raises TimeoutError if timeout_ms elapses first (0 blocks
    indefinitely)."""
    submission_id = ctypes.c_uint64()
    file_index = ctypes.c_uint32()
    range_index = ctypes.c_uint32()
    submission_done = ctypes.c_int()
    error_code = dll.fn_runai_response(
        streamer,
        ctypes.byref(submission_id),
        ctypes.byref(file_index),
        ctypes.byref(range_index),
        ctypes.byref(submission_done),
        ctypes.c_uint(timeout_ms),
    )
    if error_code == FINISHED_ERROR_CODE:
        return None
    if error_code == TIMED_OUT_ERROR_CODE:
        raise TimeoutError(
            f"runai_response timed out after {timeout_ms} ms"
        )
    return (
        error_code,
        submission_id.value,
        file_index.value,
        range_index.value,
        bool(submission_done.value),
    )


def runai_response_str(response_code: int) -> str:
    return dll.fn_runai_response_str(response_code)


def runai_list_files(
    streamer: t_streamer,
    prefix: str,
    callback: Callable[[str, int], None],
    is_recursive: bool = True,
    allow_patterns: Optional[List[str]] = None,
    ignore_patterns: Optional[List[str]] = None,
) -> None:
    # Credentials are streamer-scoped (runai_set_credentials); not passed here.
    # Exceptions raised inside a ctypes callback do not propagate through the C
    # call (they are routed to sys.unraisablehook). Capture the first one and
    # re-raise it after runai_list_files returns so callers can detect failures.
    callback_error: List[Exception] = []

    @dll.RunaiFileListCallback
    def _cb(path: bytes, size: int, _user_data: None) -> None:
        if callback_error:
            return
        try:
            callback(path.decode("utf-8"), size)
        except Exception as exc:
            callback_error.append(exc)

    def make_pattern_array(patterns):
        if not patterns:
            return None, 0
        arr = (ctypes.c_char_p * len(patterns))(
            *[p.encode("utf-8") for p in patterns]
        )
        return arr, len(patterns)

    allow_arr, num_allow = make_pattern_array(allow_patterns)
    ignore_arr, num_ignore = make_pattern_array(ignore_patterns)

    error_code = dll.fn_runai_list_files(
        streamer,
        prefix.encode("utf-8"),
        int(is_recursive),
        allow_arr, num_allow,
        ignore_arr, num_ignore,
        _cb, None,
    )
    # a callback failure is the root cause, so surface it before the error code
    if callback_error:
        raise callback_error[0]
    if error_code != SUCCESS_ERROR_CODE:
        raise ValueError(
            f"runai_list_files failed: {runai_response_str(error_code)}"
        )