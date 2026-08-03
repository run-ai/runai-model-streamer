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
    """One file plus the ranges to read from it. A range is an arbitrary (offset, size) within the
    file: ranges need not be contiguous with each other and need not be ordered.

    offsets and sizes are parallel lists rather than a list of (offset, size) pairs because they are
    exactly the C layer's range_offsets / range_sizes arrays - they pass through with no per-range
    object, which matters at a few hundred thousand tensors per model."""

    def __init__(self, id: int, path: str, offsets: List[int], sizes: List[int]) -> None:
        if len(offsets) != len(sizes):
            raise ValueError(
                f"offsets ({len(offsets)}) and sizes ({len(sizes)}) must be parallel"
            )
        self.id = id # the id of the file chunk must be unique in the context of a single stream_files request
        self.path = path
        self.offsets = offsets
        self.sizes = sizes

    @staticmethod
    def contiguous(id: int, path: str, offset: int, sizes: List[int]) -> "FileChunks":
        """The common case: sizes laid out back to back in the file, starting at offset."""
        offsets = []
        running = offset
        for size in sizes:
            offsets.append(running)
            running += size
        return FileChunks(id, path, offsets, sizes)

    def total_size(self) -> int:
        return sum(self.sizes)

    def max_chunk_size(self) -> int:
        return max(self.sizes)

    def __repr__(self) -> str:
        """Provides a clear string representation for the object."""
        return (f"FileChunks(id='{self.id}', path='{self.path}', "
                f"num_ranges={len(self.sizes)}, total_size={self.total_size()})")

class FilesRequest:
    """One submission: the files to read, plus a destination for every range.

    range_dsts is flat and in flattened range order (file 0's ranges, then file 1's, ...) - it IS the
    array handed to runai_request, and the same array response-time lookup indexes. file_base holds
    each file's first flat position, so mapping a response's (file_index, range_index) to a
    destination is O(1) rather than a prefix-sum walk per response."""

    def __init__(self) -> None:
        self.files: List[FileChunks] = []
        self.file_base: List[int] = []
        self.num_ranges = 0
        self.range_dsts: List[int] = []

    def append(self, file_chunks: FileChunks) -> None:
        self.file_base.append(self.num_ranges)
        self.num_ranges += len(file_chunks.sizes)
        self.files.append(file_chunks)

    def flat_index(self, file_index: int, range_index: int) -> int:
        return self.file_base[file_index] + range_index


class FilesRequestsIteratorWithBuffer:
    def __init__(self, buffer_size: int, files_chunks: List[FileChunks]) -> None:
        self.files_requests_iterator = FilesRequestsIterator(buffer_size, files_chunks)
        logger.debug(
            f"[RunAI Streamer] CPU Buffer size: {humanize.naturalsize(buffer_size, binary=True)} for files: {[file_chunks.path for file_chunks in files_chunks]}"
        )
        self.buffer = np.empty(buffer_size, dtype=np.uint8)
        # The buffer's base address. Destinations are absolute addresses (that is the C contract), and
        # this class packs them itself, so it can recover a range's slice of the buffer by subtracting
        # the base. That is local knowledge of its own allocation, not an assumption the range API makes.
        self.buffer_address = self.buffer.ctypes.data

    def get_global_file_and_chunk(self, local_file_index: int, local_range_index: int) -> Tuple[str, int, memoryview]:
        file_id, global_range_index = self.files_requests_iterator.get_global_file_and_chunk(
            local_file_index, local_range_index
        )
        request = self.files_requests_iterator.active_request
        start = request.range_dsts[request.flat_index(local_file_index, local_range_index)] - self.buffer_address
        size = request.files[local_file_index].sizes[local_range_index]
        return file_id, global_range_index, self.buffer[start: start + size]

    def next_request(self) -> Optional[FilesRequest]:
        request = self.files_requests_iterator.next_request()
        if request is None or len(request.files) == 0:
            return None

        # Pack this request's ranges back to back into the buffer, one absolute address per range.
        # Placement is free now (each range carries its own destination), so packing is just a running
        # cursor - no per-file sub-buffer, and no requirement that a file's ranges be adjacent.
        dsts = []
        cursor = self.buffer_address
        for file_chunks in request.files:
            for size in file_chunks.sizes:
                dsts.append(cursor)
                cursor += size
        request.range_dsts = dsts

        return request

    @staticmethod
    def with_memory_cap(
        memory_mode: MemoryCapMode,
        files_chunks: List[FileChunks],
        user_memory_limit: Optional[int] = None,
    ) -> FilesRequestsIteratorWithBuffer:
        memory_limit = 0
        if memory_mode == MemoryCapMode.unlimited:
            memory_limit = sum(file.total_size() for file in files_chunks)
        elif memory_mode == MemoryCapMode.largest_chunk:
            memory_limit = _largest_range(files_chunks)
        elif memory_mode == MemoryCapMode.limited:
            if user_memory_limit is None:
                raise RunaiStreamerMemoryLimitException(
                    f"MemoryCapMode is Limited, but no limit supplied"
                )
            largest_chunk = _largest_range(files_chunks)
            if user_memory_limit < largest_chunk:
                raise RunaiStreamerMemoryLimitException(
                    f"Memory limit supplied: {user_memory_limit} cannot be smaller than: {largest_chunk}"
                )
            memory_limit = min(user_memory_limit, sum(file.total_size() for file in files_chunks))
 
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
                self.file_to_current_chunk_index[file_chunks.id] += len(file_chunks.sizes)

        files_request = FilesRequest()
        current_request_memory_size = 0
        while self.q:
            file_chunks_iterator = self.q[0]
            file_chunks = file_chunks_iterator.next_chunks(
                self.memory_limit - current_request_memory_size
            )
            finished = file_chunks_iterator.is_finished()
            if finished:
                self.q.popleft()
            
            if len(file_chunks.sizes) == 0:
                if finished:
                    # The file itself has no range to stream (e.g. an empty
                    # safetensors shard): skip it and keep packing from the
                    # next file. Terminating here would silently drop every
                    # remaining file in the queue.
                    continue
                # The next range does not fit into this request's remaining
                # buffer budget: close this request; the file stays queued
                # for the next request.
                break

            # Note the condition above is on the RANGE COUNT, not the byte total: a file that
            # contributed only zero-sized ranges is a real contribution. The C++ layer answers a
            # zero-sized range like any other, and the caller's tensor indexing counts on exactly
            # one response per range.
            files_request.append(file_chunks)
            current_request_memory_size += file_chunks.total_size()

        if len(files_request.files) == 0:
            files_request = None
        self.active_request = files_request
        return files_request

class FileChunksIterator:
    """Hands out prefixes of a file's ranges, each fitting a caller-supplied byte budget. Ranges carry
    their own offsets now, so this is a slice of both parallel lists - no running offset to track."""

    def __init__(
        self, file_chunks: FileChunks
    ) -> None:
        self.id = file_chunks.id
        self.path = file_chunks.path
        self.offsets = file_chunks.offsets
        self.sizes = file_chunks.sizes
        self.next_index = 0

    def is_finished(self) -> bool:
        return self.next_index >= len(self.sizes)

    def next_chunks(self, size: int) -> FileChunks:
        start = self.next_index
        end = start
        remaining = size
        while end < len(self.sizes) and self.sizes[end] <= remaining:
            remaining -= self.sizes[end]
            end += 1
        self.next_index = end
        return FileChunks(self.id, self.path, self.offsets[start:end], self.sizes[start:end])

def _largest_range(files_chunks: List[FileChunks]) -> int:
    """The largest single range across all the files, or 0 when there is none.

    Both defaults are load bearing: a file may carry no ranges at all (an empty safetensors shard),
    and the file list itself may be empty (stream_files([])). This is the buffer's lower bound - a
    request must be able to hold at least one range, or next_request would return an empty request,
    which the caller reads as end of stream."""
    return max((max(file_chunks.sizes, default=0) for file_chunks in files_chunks), default=0)


def _get_memory_mode(memory_limit: Optional[str]) -> MemoryCapMode:
    if memory_limit == "-1":
        return MemoryCapMode.unlimited
    elif memory_limit == "0":
        return MemoryCapMode.largest_chunk
    else:
        return MemoryCapMode.limited
