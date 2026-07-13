from typing import Dict, List, Iterator, Optional, Tuple
from timeit import default_timer as timer
from runai_model_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_end,
    runai_request,
    runai_response,
    runai_list_files,
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
    def __init__(self) -> None:
        # Initialized here (not only in __enter__) so methods such as list_files
        # can be used without entering the context manager
        self.streamer = None
        self.s3_session = None
        self.s3_credentials = None

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


    def list_files(
        self,
        prefix: str,
        is_recursive: bool = True,
        allow_patterns: Optional[List[str]] = None,
        ignore_patterns: Optional[List[str]] = None,
        credentials: Optional[S3Credentials] = None,
    ) -> List[Tuple[str, int]]:
        # Resolve credentials through the same path as streaming: this creates and
        # retains the boto3 session (self.s3_session) and the resolved credentials
        # (self.s3_credentials) for s3 paths
        self.handle_object_store(prefix, credentials)

        params: Optional[Dict[str, str]] = None
        if self.s3_credentials is not None:
            params = {}
            if self.s3_credentials.access_key_id:
                params["key"] = self.s3_credentials.access_key_id
            if self.s3_credentials.secret_access_key:
                params["secret"] = self.s3_credentials.secret_access_key
            if self.s3_credentials.session_token:
                params["token"] = self.s3_credentials.session_token
            if self.s3_credentials.region_name:
                params["region"] = self.s3_credentials.region_name
            if self.s3_credentials.endpoint:
                params["endpoint"] = self.s3_credentials.endpoint

        results: List[Tuple[str, int]] = []
        runai_list_files(
            prefix,
            lambda path, size: results.append((path, size)),
            is_recursive=is_recursive,
            allow_patterns=allow_patterns,
            ignore_patterns=ignore_patterns,
            params=params,
        )
        return results

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
            cuda_tensor_ptrs=self.requests_iterator.cuda_tensor_ptrs if self._is_nvidia_cuda else None,
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
                    cuda_tensor_ptrs=self.requests_iterator.cuda_tensor_ptrs,
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

