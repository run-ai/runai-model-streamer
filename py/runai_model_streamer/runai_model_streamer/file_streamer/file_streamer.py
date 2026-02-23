from typing import List, Iterator, Optional
from timeit import default_timer as timer
from runai_model_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_end,
    runai_request,
    runai_response
)
from runai_model_streamer.file_streamer.requests_iterator import (
    FilesRequestsIteratorWithBuffer,
    FileChunks,
)

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    is_gs_path,
    is_azure_path,
    get_s3_credentials_module,
)

import humanize

import torch

import logging

logger = logging.getLogger(__name__)

s3_credentials_module = get_s3_credentials_module()

class RunaiStreamerInvalidInputException(Exception):
    pass

def homogeneous_paths(paths: List[str]) -> bool:
    if not paths:
        return True  # Empty list is homogeneous by default

    def path_type_fn(path: str):
        if is_s3_path(path):
            return is_s3_path
        elif is_gs_path(path):
            return is_gs_path
        elif is_azure_path(path):
            return is_azure_path
        else:
            return None

    first_type = path_type_fn(paths[0])
    for path in paths[1:]:
        if path_type_fn(path) != first_type:
            return False
    return True

class FileStreamer:
    def __enter__(self) -> "FileStreamer":
        self.streamer = runai_start()
        self.start_time = timer()
        self.total_size = 0
        self.device_str = None
        self._is_nvidia_cuda = False
        self.s3_session = None
        self.s3_credentials = None
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        size = self.total_size
        elapsed_time = timer() - self.start_time
        throughput = size / elapsed_time
        logger.info(
            f"[RunAI Streamer] Overall time to stream {humanize.naturalsize(size, binary=True)} of all files to {self.device_str}: {round(elapsed_time, 2)}s, {humanize.naturalsize(throughput, binary=True)}/s"
        )
        if self.streamer:
            runai_end(self.streamer)

    def handle_object_store(self,
                            path : str,
                            credentials : S3Credentials
    ) -> str:
        if s3_credentials_module:
            # initialize session only one
            if is_s3_path(path) and self.s3_session is None:
                # check for s3 path and init sessions and credentials           
                self.s3_session, self.s3_credentials = s3_credentials_module.get_credentials(credentials)
        return path


    def stream_files(
            self,
            file_stream_requests: List[FileChunks],
            credentials: Optional[S3Credentials] = None,
            device: Optional[str] = "cpu",
) -> None:
        if not homogeneous_paths([file_stream_request.path for file_stream_request in file_stream_requests]):
            raise RunaiStreamerInvalidInputException("Cannot stream files from multiple source types in parallel")

        self.device_str = device
        # AMD ROCm also reports "cuda" devices but has no libcuda.so — only enable the
        # direct-to-GPU C++ path for NVIDIA (torch.version.hip is set on ROCm builds).
        self._is_nvidia_cuda = (
            device is not None
            and device.startswith("cuda")
            and torch.version.hip is None
        )

        for file_stream_request in file_stream_requests:
            self.total_size += sum(file_stream_request.chunks)
            file_stream_request.path = self.handle_object_store(file_stream_request.path, credentials)

        self.requests_iterator: FilesRequestsIteratorWithBuffer = FilesRequestsIteratorWithBuffer.with_memory_mode(
            file_stream_requests, device=device
        )

        self.active_request = self.requests_iterator.next_request()
        if self.active_request is None:
            return

        runai_request(
            self.streamer,
            [file_request.path for file_request in self.active_request.files],
            [file_request.offset for file_request in self.active_request.files],
            [sum(file_request.chunks) for file_request in self.active_request.files],
            self.requests_iterator.file_buffers,
            [file_request.chunks for file_request in self.active_request.files],
            self.s3_credentials,
            cuda=self._is_nvidia_cuda,
        )

    def get_chunks(self) -> Iterator:
        if not self.streamer:
            raise ValueError("Streamer not initialized")

        if self.active_request is None:
            return

        if self._is_nvidia_cuda:
            yield from self._get_chunks_cuda()
        else:
            yield from self._get_chunks_cpu()

    def _get_chunks_cuda(self) -> Iterator:
        """Yield CUDA tensor slices as each response arrives, then fire the next batch.

        Data lands directly in the CUDA buffer via C++ cuMemcpyHtoDAsync using an
        internal thread-local pinned staging buffer. We yield all tensors from the
        current batch before reusing the buffer, so there is no aliasing.
        """
        while self.active_request is not None:
            for _ in range(sum(len(f.chunks) for f in self.active_request.files)):
                file_relative_index, chunk_relative_index = runai_response(self.streamer)
                if chunk_relative_index is None:
                    return
                file_path, chunk_index, chunk_tensor = self.requests_iterator.get_global_file_and_chunk(
                    file_relative_index, chunk_relative_index
                )
                yield file_path, chunk_index, chunk_tensor.view(1, -1)

            self.active_request = self.requests_iterator.next_request()
            if self.active_request is not None:
                runai_request(
                    self.streamer,
                    [file_request.path for file_request in self.active_request.files],
                    [file_request.offset for file_request in self.active_request.files],
                    [sum(file_request.chunks) for file_request in self.active_request.files],
                    self.requests_iterator.file_buffers,
                    [file_request.chunks for file_request in self.active_request.files],
                    self.s3_credentials,
                    cuda=True,
                )

    def _get_chunks_cpu(self) -> Iterator:
        """Yield CPU tensors as each response arrives, then fire the next batch."""
        while True:
            for _ in range(sum(len(f.chunks) for f in self.active_request.files)):
                file_relative_index, chunk_relative_index = runai_response(self.streamer)
                if chunk_relative_index is None:
                    return
                file_path, chunk_index, chunk_buffer = self.requests_iterator.get_global_file_and_chunk(
                    file_relative_index, chunk_relative_index
                )
                yield file_path, chunk_index, torch.from_numpy(chunk_buffer).view(1, -1)

            self.active_request = self.requests_iterator.next_request()
            if self.active_request is None:
                break

            runai_request(
                self.streamer,
                [file_request.path for file_request in self.active_request.files],
                [file_request.offset for file_request in self.active_request.files],
                [sum(file_request.chunks) for file_request in self.active_request.files],
                self.requests_iterator.file_buffers,
                [file_request.chunks for file_request in self.active_request.files],
                self.s3_credentials,
                cuda=False,
            )

