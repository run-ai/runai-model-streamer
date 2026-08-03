from __future__ import annotations
from typing import List, Tuple, Optional
from collections import deque
import enum
import math
import numpy as np
import os
import humanize

import logging

logger = logging.getLogger(__name__)

RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME = "RUNAI_STREAMER_MEMORY_LIMIT"
DEFAULT_MEMORY_LIMIT_STRING = "40000000000" # 40 GB (to be set to unlimited for distributed streaming)

RUNAI_STREAMER_RING_BUFFER_SIZE_BYTES_ENV_VAR_NAME = "RUNAI_STREAMER_RING_BUFFER_SIZE_BYTES"
DEFAULT_RING_BUFFER_SIZE_BYTES = 1024 * 1024 * 1024   # 1 GiB
RUNAI_STREAMER_MIN_RING_BUFFERS_ENV_VAR_NAME = "RUNAI_STREAMER_MIN_RING_BUFFERS"
DEFAULT_MIN_RING_BUFFERS = 2

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
    destination is O(1) rather than a prefix-sum walk per response.

    range_base is the other half of that mapping: each file's first GLOBAL range index, i.e. how many
    of that file's ranges earlier requests already covered. Both bases are frozen when the file is
    appended, which is what makes a request self-describing - everything needed to interpret one of
    its responses is in the request object, not in iterator state that has since moved on. That is the
    prerequisite for keeping several submissions in flight at once."""

    def __init__(self) -> None:
        self.files: List[FileChunks] = []
        self.file_base: List[int] = []
        self.range_base: List[int] = []
        self.num_ranges = 0
        self.range_dsts: List[int] = []
        # Which ring buffer this request was packed into. None once released - a request outlives its
        # buffer, since the caller may still hold the request object after the data has been consumed.
        self.buffer_index: Optional[int] = None

    def append(self, file_chunks: FileChunks, range_base: int) -> None:
        self.file_base.append(self.num_ranges)
        self.range_base.append(range_base)
        self.num_ranges += len(file_chunks.sizes)
        self.files.append(file_chunks)

    def flat_index(self, file_index: int, range_index: int) -> int:
        return self.file_base[file_index] + range_index

    def global_range_index(self, file_index: int, range_index: int) -> int:
        return self.range_base[file_index] + range_index


class FilesRequestsIteratorWithBuffer:
    def __init__(self, buffer_size: int, num_buffers: int, files_chunks: List[FileChunks]) -> None:
        self.files_requests_iterator = FilesRequestsIterator(buffer_size, files_chunks)
        self.buffer_size = buffer_size
        self.num_buffers = num_buffers
        logger.debug(
            f"[RunAI Streamer] CPU ring: {num_buffers} x {humanize.naturalsize(buffer_size, binary=True)} "
            f"= {humanize.naturalsize(buffer_size * num_buffers, binary=True)} for files: "
            f"{[file_chunks.path for file_chunks in files_chunks]}"
        )
        # ONE allocation sliced into num_buffers views. The pool is fixed at construction and never
        # grows, so a single contiguous region is simpler for the OS to manage than N separate ones -
        # and it is the shape the O_DIRECT work needs, where the base has to be page aligned.
        self.pool = np.empty(buffer_size * num_buffers, dtype=np.uint8)
        self.buffers = [self.pool[i * buffer_size: (i + 1) * buffer_size] for i in range(num_buffers)]
        # Destinations are absolute addresses (that is the C contract), and this class packs them
        # itself, so it can recover a range's slice by subtracting its buffer's base. That is local
        # knowledge of its own allocation, not an assumption the range API makes.
        self.buffer_addresses = [buffer.ctypes.data for buffer in self.buffers]
        self._free_buffers = deque(range(num_buffers))

    def has_free_buffer(self) -> bool:
        return len(self._free_buffers) > 0

    def release(self, request: FilesRequest) -> None:
        """Return a drained request's buffer to the pool. The caller must not touch any view handed
        out for that request afterwards - the next request will overwrite it."""
        if request.buffer_index is None:
            raise ValueError("request has no buffer to release (released twice?)")
        self._free_buffers.append(request.buffer_index)
        request.buffer_index = None

    def get_global_file_and_range(
        self, request: FilesRequest, local_file_index: int, local_range_index: int
    ) -> Tuple[int, int, np.ndarray]:
        file_id, global_range_index = self.files_requests_iterator.get_global_file_and_range(
            request, local_file_index, local_range_index
        )
        buffer = self.buffers[request.buffer_index]
        start = (request.range_dsts[request.flat_index(local_file_index, local_range_index)]
                 - self.buffer_addresses[request.buffer_index])
        size = request.files[local_file_index].sizes[local_range_index]
        return file_id, global_range_index, buffer[start: start + size]

    def next_request(self) -> Optional[FilesRequest]:
        if not self._free_buffers:
            raise RuntimeError("no free ring buffer - release a drained request before building another")

        request = self.files_requests_iterator.next_request()
        if request is None or len(request.files) == 0:
            return None

        # Take the buffer only once there is a request to put in it, so end of stream does not consume one.
        request.buffer_index = self._free_buffers.popleft()

        # Pack this request's ranges back to back into its buffer, one absolute address per range.
        # Placement is free now (each range carries its own destination), so packing is just a running
        # cursor - no per-file sub-buffer, and no requirement that a file's ranges be adjacent.
        dsts = []
        cursor = self.buffer_addresses[request.buffer_index]
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
        buffer_size, num_buffers = _ring_sizing(memory_mode, files_chunks, user_memory_limit)
        return FilesRequestsIteratorWithBuffer(buffer_size, num_buffers, files_chunks)

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
        
        # Per file, how many of its ranges have already been assigned to some request. Updated as each
        # request is BUILT, so nothing reads it at response time - a request carries its own frozen
        # range_base instead. Counting here rather than retroactively on the following call is what lets
        # several requests be in flight: the map belongs to request construction, not to draining.
        self.file_to_assigned_ranges = {}
        for file_chunks in files_chunks:
            self.file_to_assigned_ranges[file_chunks.id] = 0

    def get_global_file_and_range(
        self, request: FilesRequest, local_file_index: int, local_range_index: int
    ) -> Tuple[int, int]:
        # Answered entirely from the request, so a response can be resolved against its own submission
        # however far the iterator has moved on since. The first element is the caller-assigned
        # FileChunks.id, not a path.
        return (
            request.files[local_file_index].id,
            request.global_range_index(local_file_index, local_range_index),
        )

    def next_request(self) -> Optional[FilesRequest]:
        if not self.q:
            return None

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
            files_request.append(file_chunks, self.file_to_assigned_ranges[file_chunks.id])
            self.file_to_assigned_ranges[file_chunks.id] += len(file_chunks.sizes)
            current_request_memory_size += file_chunks.total_size()

        if len(files_request.files) == 0:
            files_request = None
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


def _ring_sizing(
    memory_mode: MemoryCapMode,
    files_chunks: List[FileChunks],
    user_memory_limit: Optional[int],
) -> Tuple[int, int]:
    """(buffer_size, num_buffers) for the ring.

    The BUFFER SIZE is the configured quantity and the count is derived, not the other way round.
    A single submission is already spread across every C++ worker (Assigner divides its total bytes
    between 16 filesystem / 8 object-storage workers), so more buffers add no parallelism - they only
    cover the serial gap at the request boundary. What a buffer size that is too small does cost is
    real: below roughly 32 MiB (filesystem) or 64 MiB (object storage) the workers do not even get one
    block each. Hence a configured minimum size, floored by the largest single range, with the count
    following from the memory limit."""
    largest = _largest_range(files_chunks)
    total = sum(file_chunks.total_size() for file_chunks in files_chunks)

    # Explicit minimal-memory mode: one buffer holding exactly one range. Deriving a ring here would
    # defeat the only reason to ask for this mode.
    if memory_mode == MemoryCapMode.largest_chunk:
        return largest, 1

    if memory_mode == MemoryCapMode.unlimited:
        budget = total
    else:
        if user_memory_limit is None:
            raise RunaiStreamerMemoryLimitException(
                f"MemoryCapMode is Limited, but no limit supplied"
            )
        if user_memory_limit < largest:
            raise RunaiStreamerMemoryLimitException(
                f"Memory limit supplied: {user_memory_limit} cannot be smaller than: {largest}"
            )
        budget = min(user_memory_limit, total)

    # min(configured, budget) so a limit tighter than the configured size is still honoured; max with
    # largest so a buffer always holds at least one range, or the stream cannot progress.
    buffer_size = max(largest, min(_ring_buffer_size(), budget))

    # Nothing to read at all (no ranges, or only zero-sized ones): one empty buffer, as before.
    if buffer_size == 0:
        return 0, 1

    # The floor keeps the ring a ring - at least one buffer filling behind the one being consumed.
    # It is a FLOOR, so it may exceed what the budget allows; that is deliberate, and the caller
    # logs it. ceil(total/buffer_size) stops us allocating more buffers than there is data.
    num_buffers = max(
        _min_ring_buffers(),
        min(budget // buffer_size, math.ceil(total / buffer_size)),
    )

    # The floor won: we are about to use more than the caller allowed. Say so with both numbers rather
    # than exceeding a stated limit silently - an operator who set a limit and got more than they asked
    # for has to be able to find out why from the log.
    if num_buffers * buffer_size > budget:
        logger.warning(
            f"[RunAI Streamer] CPU ring needs {num_buffers} x "
            f"{humanize.naturalsize(buffer_size, binary=True)} = "
            f"{humanize.naturalsize(num_buffers * buffer_size, binary=True)}, which exceeds the memory "
            f"limit of {humanize.naturalsize(budget, binary=True)}: "
            f"{RUNAI_STREAMER_MIN_RING_BUFFERS_ENV_VAR_NAME}={_min_ring_buffers()} is a floor, so that "
            f"many buffers are allocated regardless. Lower it to 1 to honour the limit exactly, at the "
            f"cost of the pipelining the ring provides."
        )

    return buffer_size, num_buffers


def _ring_buffer_size() -> int:
    return int(os.getenv(RUNAI_STREAMER_RING_BUFFER_SIZE_BYTES_ENV_VAR_NAME, DEFAULT_RING_BUFFER_SIZE_BYTES))


def _min_ring_buffers() -> int:
    # clamped at 1 so a hostile 0 cannot produce a ring with no buffers
    return max(1, int(os.getenv(RUNAI_STREAMER_MIN_RING_BUFFERS_ENV_VAR_NAME, DEFAULT_MIN_RING_BUFFERS)))


def _get_memory_mode(memory_limit: Optional[str]) -> MemoryCapMode:
    if memory_limit == "-1":
        return MemoryCapMode.unlimited
    elif memory_limit == "0":
        return MemoryCapMode.largest_chunk
    else:
        return MemoryCapMode.limited
