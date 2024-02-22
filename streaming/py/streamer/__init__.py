import ctypes

STREAMER_LIBRARY = "cpp/bazel-bin/streamer/streamer.so"

class DLLWrapper:
    def __init__(self, library_path):
        self.lib = ctypes.CDLL(library_path)

dll = DLLWrapper(STREAMER_LIBRARY)

dll.fn_runai_start = dll.lib.runai_start
dll.fn_runai_start.argtypes = [ctypes.c_uint32, ctypes.c_size_t, ctypes.c_size_t]
dll.fn_runai_start.restype = ctypes.c_int

dll.fn_runai_end = dll.lib.runai_end

dll.fn_runai_request = dll.lib.runai_request
dll.fn_runai_request.argtypes = [
    ctypes.c_char_p,
    ctypes.c_size_t,
    ctypes.c_size_t,
    ctypes.c_char_p,
    ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_size_t)
]
dll.fn_runai_request.restype = ctypes.c_int

dll.fn_runai_response = dll.lib.runai_response
dll.fn_runai_response.restype = ctypes.c_int
