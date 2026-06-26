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

from runai_model_streamer.cache import StreamCache

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
        self.s3_session = None
        self.s3_credentials = None
        self._cache = StreamCache()
        self._cache_original_paths: List[str] = []
        self._cache_expected_bytes: dict = {}
        self._cache_written_bytes: dict = {}
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        size = self.total_size
        elapsed_time = timer() - self.start_time
        throughput = size / elapsed_time
        logger.info(
            f"[RunAI Streamer] Overall time to stream {humanize.naturalsize(size, binary=True)} of all files to {self.device_str}: {round(elapsed_time, 2)}s, {humanize.naturalsize(throughput, binary=True)}/s"
        )
        if exc_type is not None and self._cache.enabled:
            self._cache.abort_all()
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
            enable_cache: bool = False,
) -> None:
        if not homogeneous_paths([file_stream_request.path for file_stream_request in file_stream_requests]):
            raise RunaiStreamerInvalidInputException("Cannot stream files from multiple source types in parallel")

        self.device_str = device

        self._cache_original_paths = []
        self._cache_expected_bytes = {}
        self._cache_written_bytes = {}
        for file_stream_request in file_stream_requests:
            self.total_size += sum(file_stream_request.chunks)
            self._cache_original_paths.append(file_stream_request.path)
            path = file_stream_request.path
            self._cache_expected_bytes[path] = self._cache_expected_bytes.get(path, 0) + sum(file_stream_request.chunks)
            if path not in self._cache_written_bytes:
                self._cache_written_bytes[path] = 0

        # Check cache: only use cached paths if ALL unique files hit cache (the C++ layer
        # does not support mixed local/remote paths in a single request).
        # Build lookup once to avoid TOCTOU race (file could be deleted between check and use).
        use_cache = enable_cache and self._cache.enabled
        unique_paths = list(dict.fromkeys(self._cache_original_paths))
        cache_lookup = {}
        if use_cache:
            for p in unique_paths:
                result = self._cache.cached_path_and_offset(p)
                if result is not None:
                    cache_lookup[p] = result
        all_cached = use_cache and len(cache_lookup) == len(unique_paths)

        if use_cache:
            num_files = len(self._cache_original_paths)
            if all_cached:
                logger.info(f"[RunAI Streamer][Cache] ALL {num_files} file(s) found in cache — using local paths (fast path)")
            else:
                logger.info(f"[RunAI Streamer][Cache] Cache miss for some files — streaming all {num_files} file(s) from remote")

        # Track cumulative offset per file path for cache hit offset calculation
        cache_offset_tracker = {}
        for i, file_stream_request in enumerate(file_stream_requests):
            if all_cached:
                original = self._cache_original_paths[i]
                cached_path, _ = cache_lookup[original]
                file_stream_request.path = cached_path
                # Set offset to cumulative position within the cached file
                file_stream_request.offset = cache_offset_tracker.get(original, 0)
                cache_offset_tracker[original] = cache_offset_tracker.get(original, 0) + sum(file_stream_request.chunks)
            else:
                file_stream_request.path = self.handle_object_store(file_stream_request.path, credentials)
                if use_cache:
                    self._cache.open_writer(
                        self._cache_original_paths[i],
                        file_stream_request.offset,
                        sum(file_stream_request.chunks),
                    )

        self.requests_iterator: FilesRequestsIteratorWithBuffer = FilesRequestsIteratorWithBuffer.with_memory_mode(file_stream_requests)

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
        )

    def get_chunks(self) -> Iterator:
        if not self.streamer:
            raise ValueError("Streamer not initialized")

        if self.active_request is None:
            return

        while True:
            yield from self.request_ready_chunks()

            self._cache_current_batch()

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
            )

        # Finalize all remaining cache writers after streaming completes.
        if self._cache.enabled:
            self._cache.finalize_all()

    # This function iterates over indexes of ready chunks.
    # The indexes are relative to the last request that sent
    # And need to be translated to global index in the chunks list
    def request_ready_chunks(self) -> Iterator:
        for i in range(sum(len(file_request.chunks) for file_request in self.active_request.files)):
            try:
                file_relative_index, chunk_relative_index = runai_response(self.streamer)
            except ValueError as e:
                current_files = [(f.path, f.offset, sum(f.chunks)) for f in self.active_request.files]
                logger.error(f"[RunAI Streamer][Cache] Read error. Current batch files: {current_files}")
                raise
            if chunk_relative_index == None:
                return

            file_path, chunk_index, chunk_buffer = self.requests_iterator.get_global_file_and_chunk(file_relative_index, chunk_relative_index)
            # create one dimensional tensor from the chunk buffer
            # we return a tensor of shape (1, chunk_buffer.size)
            # the data type of the original chunk_buffer, as created by the requests_iterator, is preserved (uint8)
            tensor = torch.from_numpy(chunk_buffer).view(1, -1)

            # currently file streamer is always reading a cpu buffer
            # so we don't need to move the tensor to the device
            # for future GDS/CUDA support we will need to move the tensor to the device (cpu or different device)
            if self.device_str == "cpu":
                yield file_path, chunk_index, tensor
            else:
                device_tensor = tensor.to(self.device_str)
                yield file_path, chunk_index, device_tensor

    def _cache_current_batch(self) -> None:
        """Write batch data to cache, finalize files as they complete."""
        if not self._cache.enabled or not self._cache.has_pending_writers() or self.active_request is None:
            return

        for i, file_request in enumerate(self.active_request.files):
            original_path = file_request.path

            buf = self.requests_iterator.file_buffers[i]
            size = sum(file_request.chunks)
            if size == 0:
                continue

            data = memoryview(buf)[:size]

            self._cache.append_data(original_path, data)
            self._cache_written_bytes[original_path] = self._cache_written_bytes.get(original_path, 0) + size

            # Finalize as soon as all bytes for this file are written
            if self._cache_written_bytes[original_path] >= self._cache_expected_bytes.get(original_path, 0):
                self._cache.finalize(original_path)
