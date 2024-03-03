import os
import ctypes

DEFAULT_STREAMER_LIBRARY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "lib/libstreamer.so"
)
STREAMER_LIBRARY = os.environ.get("STREAMER_LIBRARY", DEFAULT_STREAMER_LIBRARY)

t_streamer = ctypes.c_void_p


class DLLWrapper:
    def __init__(self, library_path):
        self.lib = ctypes.CDLL(library_path)


dll = DLLWrapper(STREAMER_LIBRARY)

dll.fn_runai_start = dll.lib.runai_start
dll.fn_runai_start.argtypes = [ctypes.POINTER(t_streamer)]
dll.fn_runai_start.restype = ctypes.c_int

dll.fn_runai_end = dll.lib.runai_end
dll.fn_runai_end.argtypes = [t_streamer]

dll.fn_runai_request = dll.lib.runai_request
dll.fn_runai_request.argtypes = [
    t_streamer,
    ctypes.c_char_p,
    ctypes.c_size_t,
    ctypes.c_size_t,
    ctypes.c_void_p,
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_size_t),
]
dll.fn_runai_request.restype = ctypes.c_int

dll.fn_runai_response = dll.lib.runai_response
dll.fn_runai_response.argtypes = [t_streamer, ctypes.POINTER(ctypes.c_uint32)]
dll.fn_runai_response.restype = ctypes.c_int

dll.fn_runai_response_str = dll.lib.runai_response_str
dll.fn_runai_response_str.argtypes = [ctypes.c_int]
dll.fn_runai_response_str.restype = ctypes.c_char_p
