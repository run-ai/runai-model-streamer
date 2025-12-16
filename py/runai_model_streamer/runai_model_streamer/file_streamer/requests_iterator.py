from __future__ import annotations
from typing import List, Tuple, Optional
from collections import deque
import enum
import numpy as np
import os
import humanize

import logging

logger = logging.getLogger(__name__)

RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME = "RUNAI_STREAMER_MEMORY_LIMIT"
DEFAULT_MEMORY_LIMIT_STRING = "40000000000" # 40 GB (to be set to unlimited for distributed streaming)

class RunaiStreamerMemoryLimitException(Exception):
    pass


class MemoryCapMode(enum.Enum):
    unlimited = 1
    limited = 2
    largest_chunk = 3

class FileChunks:
    def __init__(self, id: int, path: str, offset: int, chunks: List[int]) -> None:
        self.id = id # the id of the file chunk must be unique in the context of a single stream_files request
        self.path = path
        self.offset = offset
        self.chunks = chunks

    def total_size(self) -> int:
        return sum(self.chunks)

    def max_chunk_size(self) -> int:
        return max(self.chunks)

    def __repr__(self) -> str:
        """Provides a clear string representation for the object."""
        return (f"FileChunks(id='{self.id}', path='{self.path}', offset={self.offset}, "
                f"num_chunks={len(self.chunks)}, total_size={self.total_size()})")

class FilesRequest:
    def __init__(self) -> None:
        self.files: List[FileChunks] = []

    def append(self, file_chunks: FileChunks) -> None:
        self.files.append(file_chunks)


class FilesRequestsIteratorWithBuffer:
    def __init__(self, buffer_size: int, files_chunks: List[FileChunks]) -> None:
        self.files_requests_iterator = FilesRequestsIterator(buffer_size, files_chunks)
        logger.debug(
            f"[RunAI Streamer] CPU Buffer size: {humanize.naturalsize(buffer_size, binary=True)} for files: {[file_chunks.path for file_chunks in files_chunks]}"
        )
        self.buffer = np.empty(buffer_size, dtype=np.uint8)
        self.file_buffers = []

    def get_global_file_and_chunk(self, local_file_index: int, local_chunk_index: int) -> Tuple[str, int, memoryview]:
        file_id, global_chunk_index = self.files_requests_iterator.get_global_file_and_chunk(
            local_file_index, local_chunk_index
        )
        file_buffer = self.file_buffers[local_file_index]

        file_active_chunks = self.files_requests_iterator.active_request.files[local_file_index].chunks
        chunk_offset_start = sum(file_active_chunks[:local_chunk_index])
        chunk_offset_end = chunk_offset_start + file_active_chunks[local_chunk_index]
        return file_id, global_chunk_index, file_buffer[chunk_offset_start: chunk_offset_end]

    def next_request(self) -> Optional[FilesRequest]:
        next_requests = self.files_requests_iterator.next_request()
        if next_requests is None or len(next_requests.files) == 0:
            return None

        self.file_buffers = []
        global_buffer_offset = 0
        for file_request in next_requests.files:
            chunks_size = sum(file_request.chunks)
            self.file_buffers.append(self.buffer[global_buffer_offset: global_buffer_offset + chunks_size])
            global_buffer_offset += chunks_size
            
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
            largest_chunk = max((max(file_chunks.chunks, default=0) for file_chunks in files_chunks), default=0)
            if user_memory_limit < largest_chunk:
                raise RunaiStreamerMemoryLimitException(
                    f"Memory limit supplied: {user_memory_limit} cannot be smaller than: {largest_chunk}"
                )
            memory_limit = min(user_memory_limit, sum(sum(file.chunks) for file in files_chunks))
 
        return FilesRequestsIteratorWithBuffer(memory_limit, files_chunks)

    @staticmethod
    def with_memory_mode(
        files_chunks: List[FileChunks],
    ) -> FilesRequestsIteratorWithBuffer:
        memory_limit = os.getenv(RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME)
        if memory_limit is None:
            memory_limit = DEFAULT_MEMORY_LIMIT_STRING
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
        
        self.file_to_current_chunk_index = {}
        for file_chunks in files_chunks:
            self.file_to_current_chunk_index[file_chunks.id] = 0

        self.active_request: FilesRequest = None

    def get_global_file_and_chunk(self, local_file_index: int, local_chunk_index: int) -> Tuple[str, int]:
        file_id = self.active_request.files[local_file_index].id
        return file_id, self.file_to_current_chunk_index[file_id] + local_chunk_index

    def next_request(self) -> Optional[FilesRequest]:
        if not self.q:
            return None
        
        if self.active_request is not None:
            for file_chunks in self.active_request.files:
                self.file_to_current_chunk_index[file_chunks.id] += len(file_chunks.chunks)
            
        files_request = FilesRequest()
        current_request_memory_size = 0
        while self.q:
            file_chunks_iterator = self.q[0]
            file_chunks = file_chunks_iterator.next_chunks(
                self.memory_limit - current_request_memory_size
            )
            if file_chunks_iterator.is_finished():
                self.q.popleft()
            
            if len(file_chunks.chunks) == 0 or sum(file_chunks.chunks) == 0:
                break

            files_request.append(file_chunks)
            current_request_memory_size += sum(file_chunks.chunks)

        if len(files_request.files) == 0:
            files_request = None
        self.active_request = files_request
        return files_request

class FileChunksIterator:

    def __init__(
        self, file_chunks: FileChunks
    ) -> None:
        self.id = file_chunks.id
        self.path = file_chunks.path
        self.next_request_offset = file_chunks.offset
        self.chunks_iterator = ChunksIterator(file_chunks.chunks)

    def is_finished(self) -> bool:
        return self.chunks_iterator.is_finished()

    def next_chunks(self, size: int) -> FileChunks:
        chunks = self.chunks_iterator.next_chunks(size)
        starting_offset = self.next_request_offset
        self.next_request_offset += sum(chunks)
        return FileChunks(self.id, self.path, starting_offset, chunks)

class ChunksIterator:
    def __init__(
        self, chunks: List[int]
    ) -> None:
        self.q = deque(chunks)

    def is_finished(self) -> bool:
        return len(self.q) == 0

    def next_chunks(self, size: int) -> List[int]:
        chunks = []
        current_request_size = 0

        while not self.is_finished():
            candidate_chunk = self.q[0]
            if current_request_size + candidate_chunk > size:
                return chunks

            chunks.append(self.q.popleft())
            current_request_size += candidate_chunk

        return chunks

def _get_memory_mode(memory_limit: str | None) -> MemoryCapMode:
    if memory_limit == "-1":
        return MemoryCapMode.unlimited
    elif memory_limit == "0":
        return MemoryCapMode.largest_chunk
    else:
        return MemoryCapMode.limited
