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

        # Set the streamer's object-storage credentials as a key/value dict (canonical config-param keys).
        self.fn_runai_set_credentials = self.lib.runai_set_credentials
        self.fn_runai_set_credentials.argtypes = [
            t_streamer,
            ctypes.POINTER(ctypes.c_char_p),                 # param_keys
            ctypes.POINTER(ctypes.c_char_p),                 # param_values
            ctypes.c_uint,                                   # num_params
        ]
        self.fn_runai_set_credentials.restype = ctypes.c_int

        # Multi-request submit: credentials are streamer-scoped (runai_set_credentials), not passed here; the
        # assigned submission id is returned via out_submission_id.
        self.fn_runai_request = self.lib.runai_request
        self.fn_runai_request.argtypes = [
            t_streamer,
            ctypes.POINTER(ctypes.c_uint32),                 # out_submission_id
            ctypes.c_uint32,                                 # num_files
            ctypes.POINTER(ctypes.c_char_p),                 # paths
            ctypes.POINTER(ctypes.c_size_t),                 # file_offsets
            ctypes.POINTER(ctypes.c_size_t),                 # bytesizes
            ctypes.POINTER(ctypes.c_void_p),                 # dsts
            ctypes.POINTER(ctypes.c_uint32),                 # num_sizes
            ctypes.POINTER(ctypes.POINTER(ctypes.c_size_t)), # internal_sizes
        ]
        self.fn_runai_request.restype = ctypes.c_int

        # Multi-request response: also reports the owning submission id, whether that submission is now
        # complete (submission_done), and takes a timeout (0 = block indefinitely).
        self.fn_runai_response = self.lib.runai_response
        self.fn_runai_response.argtypes = [
            t_streamer,
            ctypes.POINTER(ctypes.c_uint32),                 # out_submission_id
            ctypes.POINTER(ctypes.c_uint32),                 # file_index
            ctypes.POINTER(ctypes.c_uint32),                 # index
            ctypes.POINTER(ctypes.c_int),                    # submission_done
            ctypes.c_uint,                                   # timeout_ms
        ]
        self.fn_runai_response.restype = ctypes.c_int

        self.fn_runai_response_str = self.lib.runai_response_str
        self.fn_runai_response_str.argtypes = [ctypes.c_int]
        self.fn_runai_response_str.restype = ctypes.c_char_p

        RunaiFileListCallback = ctypes.CFUNCTYPE(
            None,                # return void
            ctypes.c_char_p,     # path
            ctypes.c_size_t,     # file_size
            ctypes.c_void_p,     # user_data
        )
        self.RunaiFileListCallback = RunaiFileListCallback

        self.fn_runai_list_files = self.lib.runai_list_files
        self.fn_runai_list_files.argtypes = [
            t_streamer,                              # streamer
            ctypes.c_char_p,                         # prefix
            ctypes.c_int,                            # is_recursive
            ctypes.POINTER(ctypes.c_char_p),         # allow_patterns
            ctypes.c_uint,                           # num_allow_patterns
            ctypes.POINTER(ctypes.c_char_p),         # ignore_patterns
            ctypes.c_uint,                           # num_ignore_patterns
            RunaiFileListCallback,                   # callback
            ctypes.c_void_p,                         # user_data
        ]
        self.fn_runai_list_files.restype = ctypes.c_int


dll = LibstreamerDLLWrapper(STREAMER_LIBRARY)
