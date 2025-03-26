import os
from typing import List, Iterator, Optional
import numpy as np
from timeit import default_timer as timer
from runai_model_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_end,
    runai_read,
    runai_request,
    runai_response,
)
from runai_model_streamer.file_streamer.requests_iterator import (
    RequestsIterator,
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

    def stream_file(
            self,
            path: str,
            file_offset: int,
            chunks: List[int],
            credentials: Optional[S3Credentials] = None,
) -> None:
        self.total_size = self.total_size + sum(chunks)
        self.path = self.handle_object_store(path, credentials)

        self.requests_iterator, buffer_size = RequestsIterator.with_memory_mode(
            file_offset, chunks
        )
        print(
            f"[RunAI Streamer] CPU Buffer size: {humanize.naturalsize(buffer_size, binary=True)} for file: {os.path.basename(path)}",
            flush=True,
        )

        self.dst_buffer = np.empty(buffer_size, dtype=np.uint8)
 
        request = self.requests_iterator.next_request()
        self.current_request_chunks = request.chunks
        runai_request(
            self.streamer,
            self.path,
            request.file_offset,
            sum(request.chunks),
            self.dst_buffer,
            request.chunks,
            self.s3_credentials,
        )

    def get_chunks(self) -> Iterator:
        if not self.streamer:
            raise ValueError("Streamer not initialized")

        chunk_index_base = 0
        while True:
            for relative_index, buffer, buffer_offset in self.request_ready_chunks():
                yield chunk_index_base + relative_index, buffer, buffer_offset

            chunk_index_base = self.requests_iterator.current_chunk_index
            next_request = self.requests_iterator.next_request()
            if next_request is None:
                break
            self.current_request_chunks = next_request.chunks
            runai_request(
                self.streamer,
                self.path,
                next_request.file_offset,
                sum(next_request.chunks),
                self.dst_buffer,
                next_request.chunks,
            )

    # This function iterates over indexes of ready chunks.
    # The indexes are relative to the last request that sent
    # And need to be translated to global index in the chunks list
    def request_ready_chunks(self) -> Iterator:
        for i in range(len(self.current_request_chunks)):
            relative_index = runai_response(self.streamer)
            if relative_index == None:
                return
            yield relative_index, self.dst_buffer, sum(
                self.current_request_chunks[:relative_index]
            )

