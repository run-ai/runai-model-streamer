import os
import ctypes

DEFAULT_STREAMER_LIBRARY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "lib/libstreamer.so"
)
STREAMER_LIBRARY = os.environ.get("STREAMER_LIBRARY", DEFAULT_STREAMER_LIBRARY)

t_streamer = ctypes.c_void_p


class LibstreamerDLLWrapper:
    def __init__(self, library_path):
        self.lib = ctypes.CDLL(library_path)

        self.fn_runai_start = self.lib.runai_start
        self.fn_runai_start.argtypes = [ctypes.POINTER(t_streamer)]
        self.fn_runai_start.restype = ctypes.c_int

        self.fn_runai_end = self.lib.runai_end
        self.fn_runai_end.argtypes = [t_streamer]

        self.fn_runai_read = self.lib.runai_read
        self.fn_runai_read.argtypes = [
            t_streamer,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_void_p,
        ]
        self.fn_runai_read.restype = ctypes.c_int

        self.fn_runai_request = self.lib.runai_request
        self.fn_runai_request.argtypes = [
            t_streamer,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        self.fn_runai_request.restype = ctypes.c_int

        self.fn_runai_response = self.lib.runai_response
        self.fn_runai_response.argtypes = [t_streamer, ctypes.POINTER(ctypes.c_uint32)]
        self.fn_runai_response.restype = ctypes.c_int

        self.fn_runai_response_str = self.lib.runai_response_str
        self.fn_runai_response_str.argtypes = [ctypes.c_int]
        self.fn_runai_response_str.restype = ctypes.c_char_p

        self.fn_runai_list = self.lib.runai_list
        self.fn_runai_list.argtypes = [
            t_streamer,
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
            ctypes.POINTER(ctypes.c_size_t)
        ]
        self.fn_runai_list.restype = ctypes.c_int

        self.fn_runai_free_list = self.lib.runai_free_list
        self.fn_runai_free_list.argtypes = [
            t_streamer,
            ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
            ctypes.c_size_t
        ]
        self.fn_runai_free_list.restype = ctypes.c_int

        self.fn_runai_read_object_to_file = self.lib.runai_read_object_to_file
        self.fn_runai_read_object_to_file.argtypes = [
            t_streamer,
            ctypes.c_char_p,
            ctypes.c_char_p
        ]
        self.fn_runai_read_object_to_file.restype = ctypes.c_int


dll = LibstreamerDLLWrapper(STREAMER_LIBRARY)
