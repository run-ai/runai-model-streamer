from __future__ import annotations
from typing import List, Tuple, Optional
from collections import deque
import ctypes
import enum
import numpy as np
import os
import humanize
import torch

import logging

logger = logging.getLogger(__name__)

RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME = "RUNAI_STREAMER_MEMORY_LIMIT"
DEFAULT_MEMORY_LIMIT_STRING = "40000000000" # 40 GB (to be set to unlimited for distributed streaming)

RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR = "RUNAI_STREAMER_CUDA_ALIGNMENT"
DEFAULT_CUDA_ALIGNMENT = 256

def align_up(size: int, alignment: int) -> int:
    if alignment <= 1:
        return size
    return ((size + alignment - 1) // alignment) * alignment

def get_cuda_alignment() -> int:
    """Return the CUDA buffer alignment in bytes (default 256). Set to 0 or 1 to disable."""
    val = int(os.getenv(RUNAI_STREAMER_CUDA_ALIGNMENT_ENV_VAR, str(DEFAULT_CUDA_ALIGNMENT)))
    return val if val > 1 else 1

class RunaiStreamerMemoryLimitException(Exception):
    pass


class MemoryCapMode(enum.Enum):
    unlimited = 1
    limited = 2
    largest_chunk = 3

class FileChunks:
    def __init__(self, id: int, path: str, offset: int, chunks: List[int],
                 buffer_strides: Optional[List[int]] = None) -> None:
        self.id = id # the id of the file chunk must be unique in the context of a single stream_files request
        self.path = path
        self.offset = offset
        self.chunks = chunks
        # buffer_strides: padded sizes for each chunk in the GPU buffer.
        # When None, effective_strides == chunks (no padding, backward-compatible).
        self.buffer_strides = buffer_strides

    @property
    def effective_strides(self) -> List[int]:
        return self.buffer_strides if self.buffer_strides is not None else self.chunks

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
    def __init__(self, buffer_size: int, files_chunks: List[FileChunks], device: str = "cpu") -> None:
        self.files_requests_iterator = FilesRequestsIterator(buffer_size, files_chunks)
        # Only use the direct-to-GPU path for NVIDIA GPUs (requires libcuda.so).
        # AMD ROCm also reports "cuda" devices but has no libcuda; torch.version.hip
        # is set on ROCm builds, so we fall back to the CPU pageable path there.
        self._is_nvidia_cuda = (
            device is not None
            and device.startswith("cuda")
            and torch.version.hip is None
        )
        if self._is_nvidia_cuda:
            # CUDA device buffer: C++ writes directly via cuMemcpyHtoDAsync using
            # an internal thread-local pinned staging buffer (2 MB per worker).
            # Over-allocate by (alignment - 1) bytes so we can slice to an aligned
            # start address, guaranteeing that every per-tensor offset (which is a
            # multiple of `alignment`) also lands on an aligned address.
            alignment = get_cuda_alignment()
            if alignment > 1:
                _raw = torch.empty(buffer_size + alignment - 1, dtype=torch.uint8, device=device)
                aligned_start = (-_raw.data_ptr()) % alignment
                self.buffer = _raw[aligned_start : aligned_start + buffer_size]
            else:
                self.buffer = torch.empty(buffer_size, dtype=torch.uint8, device=device)
            logger.debug(
                f"[RunAI Streamer] CUDA buffer size: {humanize.naturalsize(buffer_size, binary=True)} "
                f"on {device} for files: {[fc.path for fc in files_chunks]}"
            )
        else:
            # CPU pageable buffer: used for CPU targets and as fallback for AMD ROCm.
            self.buffer = np.empty(buffer_size, dtype=np.uint8)
            logger.debug(
                f"[RunAI Streamer] CPU buffer size: {humanize.naturalsize(buffer_size, binary=True)} "
                f"for files: {[fc.path for fc in files_chunks]}"
            )
        self.file_buffers: List = []
        # Flat list of per-tensor aligned GPU ctypes pointers (CUDA only).
        self.cuda_tensor_ptrs: List[ctypes.c_void_p] = []

    def get_global_file_and_chunk(self, local_file_index: int, local_chunk_index: int) -> Tuple:
        file_id, global_chunk_index = self.files_requests_iterator.get_global_file_and_chunk(
            local_file_index, local_chunk_index
        )
        active_file = self.files_requests_iterator.active_request.files[local_file_index]
        # Use padded strides to compute where in the buffer this tensor starts,
        # but return only the actual (unpadded) tensor bytes.
        strides = active_file.effective_strides
        padded_offset = sum(strides[:local_chunk_index])
        actual_size = active_file.chunks[local_chunk_index]
        return file_id, global_chunk_index, self.file_buffers[local_file_index][padded_offset:padded_offset + actual_size]

    def next_request(self) -> Optional[FilesRequest]:
        next_requests = self.files_requests_iterator.next_request()
        if next_requests is None or len(next_requests.files) == 0:
            return None

        self.file_buffers = []
        self.cuda_tensor_ptrs = []
        global_buffer_offset = 0
        for file_request in next_requests.files:
            # Use padded strides to determine how much buffer space this file occupies.
            padded_total = sum(file_request.effective_strides)
            self.file_buffers.append(self.buffer[global_buffer_offset: global_buffer_offset + padded_total])
            if self._is_nvidia_cuda:
                # Build a per-tensor GPU pointer list for this file.
                tensor_offset = 0
                for stride in file_request.effective_strides:
                    ptr = ctypes.c_void_p(self.buffer.data_ptr() + global_buffer_offset + tensor_offset)
                    self.cuda_tensor_ptrs.append(ptr)
                    tensor_offset += stride
            global_buffer_offset += padded_total

        return next_requests

    @staticmethod
    def with_memory_cap(
        memory_mode: MemoryCapMode,
        files_chunks: List[FileChunks],
        user_memory_limit: Optional[int] = None,
        device: str = "cpu",
    ) -> FilesRequestsIteratorWithBuffer:
        memory_limit = 0
        if memory_mode == MemoryCapMode.unlimited:
            # Use padded strides for the total size so the buffer is large enough.
            total_size = sum(sum(file.effective_strides) for file in files_chunks)
            is_nvidia_cuda = (
                device is not None
                and device.startswith("cuda")
                and torch.version.hip is None
            )
            if is_nvidia_cuda and torch.cuda.is_available():
                # For NVIDIA CUDA, cap the buffer at available GPU memory so that we
                # don't OOM during allocation. If the model is larger than free VRAM,
                # the buffer is reused in chunks just like the limited-memory path.
                free_gpu, _ = torch.cuda.mem_get_info(device)
                memory_limit = min(total_size, free_gpu)
            else:
                memory_limit = total_size
        elif memory_mode == MemoryCapMode.largest_chunk:
            memory_limit = max(max(file_chunks.effective_strides) for file_chunks in files_chunks)
        elif memory_mode == MemoryCapMode.limited:
            if user_memory_limit is None:
                raise RunaiStreamerMemoryLimitException(
                    f"MemoryCapMode is Limited, but no limit supplied"
                )
            largest_chunk = max((max(file_chunks.effective_strides, default=0) for file_chunks in files_chunks), default=0)
            if user_memory_limit < largest_chunk:
                raise RunaiStreamerMemoryLimitException(
                    f"Memory limit supplied: {user_memory_limit} cannot be smaller than: {largest_chunk}"
                )
            memory_limit = min(user_memory_limit, sum(sum(file.effective_strides) for file in files_chunks))

        return FilesRequestsIteratorWithBuffer(memory_limit, files_chunks, device=device)

    @staticmethod
    def with_memory_mode(
        files_chunks: List[FileChunks],
        device: str = "cpu",
    ) -> FilesRequestsIteratorWithBuffer:
        memory_limit = os.getenv(RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME)
        if memory_limit is None:
            memory_limit = DEFAULT_MEMORY_LIMIT_STRING
        memory_mode = _get_memory_mode(memory_limit)
        if memory_limit is not None:
            memory_limit = int(memory_limit)
        return FilesRequestsIteratorWithBuffer.with_memory_cap(
            memory_mode, files_chunks, memory_limit, device=device
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
            # Account for padded buffer space in the memory limit.
            current_request_memory_size += sum(file_chunks.effective_strides)

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
        self.chunks_iterator = ChunksIterator(file_chunks.chunks, file_chunks.effective_strides)

    def is_finished(self) -> bool:
        return self.chunks_iterator.is_finished()

    def next_chunks(self, size: int) -> FileChunks:
        chunks, strides = self.chunks_iterator.next_chunks(size)
        starting_offset = self.next_request_offset
        self.next_request_offset += sum(chunks)
        # Preserve buffer_strides only when they differ from chunks.
        buffer_strides = strides if strides != chunks else None
        return FileChunks(self.id, self.path, starting_offset, chunks, buffer_strides)

class ChunksIterator:
    def __init__(
        self, chunks: List[int], strides: Optional[List[int]] = None
    ) -> None:
        self.q = deque(chunks)
        # strides_q holds the padded sizes used for memory-limit accounting.
        # When strides equal chunks there is no padding and we use a single queue.
        self.strides_q: Optional[deque] = (
            deque(strides) if strides is not None and strides != chunks else None
        )

    def is_finished(self) -> bool:
        return len(self.q) == 0

    def next_chunks(self, size: int) -> Tuple[List[int], List[int]]:
        """Return (chunks, strides) that fit within `size` bytes of buffer space."""
        chunks = []
        strides = []
        current_request_size = 0

        while not self.is_finished():
            candidate_chunk = self.q[0]
            candidate_stride = self.strides_q[0] if self.strides_q else candidate_chunk
            if current_request_size + candidate_stride > size:
                return chunks, strides

            chunks.append(self.q.popleft())
            if self.strides_q:
                strides.append(self.strides_q.popleft())
            else:
                strides.append(candidate_chunk)
            current_request_size += candidate_stride

        return chunks, strides

def _get_memory_mode(memory_limit: str | None) -> MemoryCapMode:
    if memory_limit == "-1":
        return MemoryCapMode.unlimited
    elif memory_limit == "0":
        return MemoryCapMode.largest_chunk
    else:
        return MemoryCapMode.limited
