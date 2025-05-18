from typing import List, Iterator, Optional
import numpy as np
from timeit import default_timer as timer
from runai_model_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_end,
    runai_read,
    runai_request_multi,
    runai_response_multi
)
from runai_model_streamer.file_streamer.requests_iterator import (
    FilesRequestsIteratorWithBuffer,
    FileChunks,
)

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    is_gs_path,
    set_gs_environment_variables,
    convert_gs_path,
    get_s3_credentials_module,
)

import humanize

s3_credentials_module = get_s3_credentials_module()

class FileStreamer:
    def __enter__(self) -> "FileStreamer":
        self.streamer = runai_start()
        self.start_time = timer()
        self.total_size = 0

        self.s3_session = None
        self.s3_credentials = None
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        size = self.total_size
        elapsed_time = timer() - self.start_time
        throughput = size / elapsed_time
        print(
            f"[RunAI Streamer] Overall time to stream {humanize.naturalsize(size, binary=True)} of all files: {round(elapsed_time, 2)}s, {humanize.naturalsize(throughput, binary=True)}/s",
            flush=True,
        )
        if self.streamer:
            runai_end(self.streamer)

    def handle_object_store(self,
                            path : str,
                            credentials : S3Credentials
    ) -> str:
        if s3_credentials_module:
            # initialize session only one

            if is_gs_path(path):
                # set gs endpoint
                set_gs_environment_variables()
                # replace path prefix
                path = convert_gs_path(path)
            elif is_s3_path(path) and self.s3_session is None:
                # check for s3 path and init sessions and credentials           
                self.s3_session, self.s3_credentials = s3_credentials_module.get_credentials(credentials)
        return path

    def read_file(
            self,
            path: str,
            offset: int,
            len: int,
            credentials: Optional[S3Credentials] = None,
    ) -> memoryview:
        dst_buffer = np.empty(len, dtype=np.uint8)
      
        path = self.handle_object_store(path, credentials)
        runai_read(
            self.streamer,
            path,
            offset,
            len,
            dst_buffer,
            self.s3_credentials,
        )
        return dst_buffer

    def stream_files(
            self,
            file_stream_requests: List[FileChunks],
            credentials: Optional[S3Credentials] = None,
) -> None:
        self.total_size = self.total_size + sum(sum(file_stream_request.chunks) for file_stream_request in file_stream_requests)
        for file_stream_request in file_stream_requests:
            file_stream_request.path = self.handle_object_store(file_stream_request.path, credentials)

        self.requests_iterator = FilesRequestsIteratorWithBuffer.with_memory_mode(file_stream_requests)

        self.file_to_current_chunk_index = {}
        for file_stream_request in file_stream_requests:
            self.file_to_current_chunk_index[file_stream_request.path] = 0

 
        self.active_request = self.requests_iterator.next_request()
        if self.active_request is None:
            return 

        runai_request_multi(
            self.streamer,
            [file_request.path for file_request in self.active_request.files],
            [file_request.offset for file_request in self.active_request.files],
            [sum(file_request.chunks) for file_request in self.active_request.files],
            self.requests_iterator.file_buffers,
            [file_request.chunks for file_request in self.active_request.files],
            self.s3_credentials,
        )

    def get_chunks(self) -> Iterator:
        if not self.streamer:
            raise ValueError("Streamer not initialized")
        
        if self.active_request is None:
            return 
        
        
        while True:
            yield from self.request_ready_chunks()

            for active_request_file in self.active_request.files:
                self.file_to_current_chunk_index[active_request_file.path] += len(active_request_file.chunks)
            
            self.active_request = self.requests_iterator.next_request()
            if self.active_request is None:
                break

            runai_request_multi(
                self.streamer,
                [file_request.path for file_request in self.active_request.files],
                [file_request.offset for file_request in self.active_request.files],
                [sum(file_request.chunks) for file_request in self.active_request.files],
                self.requests_iterator.file_buffers,
                [file_request.chunks for file_request in self.active_request.files],
                self.s3_credentials,
            )

    # This function iterates over indexes of ready chunks.
    # The indexes are relative to the last request that sent
    # And need to be translated to global index in the chunks list
    def request_ready_chunks(self) -> Iterator:
        for i in range(sum(len(file_request.chunks) for file_request in self.active_request.files)):
            file_relative_index, chunk_relative_index = runai_response_multi(self.streamer)
            if chunk_relative_index == None:
                return
            
            file_path = self.active_request.files[file_relative_index].path
            file_current_chunk_index = self.file_to_current_chunk_index[file_path]
            yield file_path, file_current_chunk_index + chunk_relative_index, self.requests_iterator.file_buffers[file_relative_index], sum(self.active_request.files[file_relative_index].chunks[:chunk_relative_index])

