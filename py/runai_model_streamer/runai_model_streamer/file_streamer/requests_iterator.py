from __future__ import annotations
from typing import List, Tuple, Optional
from collections import deque
import enum
import numpy as np
import os
import humanize


RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME = "RUNAI_STREAMER_MEMORY_LIMIT"


class RunaiStreamerMemoryLimitException(Exception):
    pass


class MemoryCapMode(enum.Enum):
    unlimited = 1
    limited = 2
    largest_chunk = 3

class FileChunks:
    def __init__(self, path: str, offset: int, chunks: List[int]) -> None:
        self.path = path
        self.offset = offset
        self.chunks = chunks

class FilesRequest:
    def __init__(self) -> None:
        self.files: List[FileChunks] = []

    def append(self, file_chunks: FileChunks) -> None:
        self.files.append(file_chunks)


class FilesRequestsIteratorWithBuffer:
    def __init__(self, buffer_size: int, files_chunks: List[FileChunks]) -> None:
        self.files_requests_iterator = FilesRequestsIterator(buffer_size, files_chunks)
        print(
            f"[RunAI Streamer] CPU Buffer size: {humanize.naturalsize(buffer_size, binary=True)} for files: {[os.path.basename(file_chunks.path) for file_chunks in files_chunks]}",
            flush=True,
        )
        self.buffer = np.empty(buffer_size, dtype=np.uint8)
        self.file_buffers = []

    def next_request(self) -> Optional[FilesRequest]:
        next_requests = self.files_requests_iterator.next_request()
        if next_requests is None:
            return None

        self.file_buffers = []
        current_buffer_index = 0
        for file_request in next_requests.files:
            self.file_buffers.append(self.buffer[current_buffer_index: current_buffer_index + sum(file_request.chunks)])
            current_buffer_index += sum(file_request.chunks)
            
        return next_requests

    @staticmethod
    def with_memory_cap(
        memory_mode: MemoryCapMode,
        files_chunks: List[FileChunks],
        user_memory_limit: Optional[int] = None,
    ) -> FilesRequestsIteratorWithBuffer:
        memory_limit = 0
        if memory_mode == MemoryCapMode.unlimited:
            memory_limit = sum(sum(file.chunks) for file in files_chunks)
        elif memory_mode == MemoryCapMode.largest_chunk:
            memory_limit = max(max(file_chunks.chunks) for file_chunks in files_chunks)
        elif memory_mode == MemoryCapMode.limited:
            if user_memory_limit is None:
                raise RunaiStreamerMemoryLimitException(
                    f"MemoryCapMode is Limited, but no limit supplied"
                )
            largest_chunk = max(max(file_chunks.chunks) for file_chunks in files_chunks)
            if user_memory_limit < largest_chunk:
                raise RunaiStreamerMemoryLimitException(
                    f"Memory limit supplied: {user_memory_limit} cannot be smaller than: {largest_chunk}"
                )
            memory_limit = user_memory_limit

        return FilesRequestsIteratorWithBuffer(memory_limit, files_chunks)

    @staticmethod
    def with_memory_mode(
        files_chunks: List[FileChunks],
    ) -> FilesRequestsIteratorWithBuffer:
        memory_limit = os.getenv(RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME)
        memory_mode = _get_memory_mode(memory_limit)
        if memory_limit is not None:
            memory_limit = int(memory_limit)
        return FilesRequestsIteratorWithBuffer.with_memory_cap(
            memory_mode, files_chunks, memory_limit
        )

class FilesRequestsIterator:
    def __init__(self, memory_limit: int, files_chunks: List[FileChunks]) -> None:
        self.memory_limit = memory_limit
        self.q = deque(FileChunksIterator(file_chunks)
            for file_chunks in files_chunks)
        
    def next_request(self) -> Optional[FilesRequest]:
        if not self.q:
            return None
            
        files_request = FilesRequest()
        current_request_memory_size = 0
        while self.q:
            file_chunks_iterator = self.q[0]
            file_chunks, is_finished = file_chunks_iterator.next_chunks(
                self.memory_limit - current_request_memory_size
            )
            if is_finished:
                self.q.popleft()
            elif len(file_chunks.chunks) == 0:
                break
            files_request.append(file_chunks)
            current_request_memory_size += sum(file_chunks.chunks)

        return files_request

class FileChunksIterator:
    def __init__(
        self, file_chunks: FileChunks
    ) -> None:
        self.path = file_chunks.path
        self.next_request_offset = file_chunks.offset
        self.chunks_iterator = ChunksIterator(file_chunks.chunks)

    def next_chunks(self, memory_limit: int) -> Tuple[FileChunks, bool]:
        chunks, is_finished = self.chunks_iterator.next_chunks(memory_limit)
        starting_offset = self.next_request_offset
        self.next_request_offset += sum(chunks)
        return FileChunks(self.path, starting_offset, chunks), is_finished

class ChunksIterator:
    def __init__(
        self, chunks: List[int]
    ) -> None:
        self.q = deque(chunks)

    def next_chunks(self, memory_limit: int) -> Tuple[List[int], bool]:
        chunks = []
        current_request_memory_size = 0

        while self.q:
            candidate_chunk = self.q[0]
            if current_request_memory_size + candidate_chunk > memory_limit:
                return chunks, False

            chunks.append(self.q.popleft())
            current_request_memory_size += candidate_chunk

        return chunks, True


def _get_memory_mode(memory_limit: str | None) -> MemoryCapMode:
    if memory_limit is None or memory_limit == "-1":
        return MemoryCapMode.unlimited
    elif memory_limit == "0":
        return MemoryCapMode.largest_chunk
    else:
        return MemoryCapMode.limited
