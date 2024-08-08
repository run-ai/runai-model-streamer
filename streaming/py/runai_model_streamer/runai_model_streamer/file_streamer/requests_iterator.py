from __future__ import annotations
from typing import List, Tuple, Optional
import enum
import os


RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME = "RUNAI_STREAMER_MEMORY_LIMIT"


class RunaiStreamerMemoryLimitException(Exception):
    pass


class MemoryCapMode(enum.Enum):
    unlimited = 1
    limited = 2
    largest_chunk = 3


class Request:
    def __init__(self, file_offset: int, chunks: List[int]) -> None:
        self.file_offset = file_offset
        self.chunks = chunks


class RequestsIterator:
    def __init__(
        self, memory_limit: int, initial_file_offset: int, chunks: List[int]
    ) -> None:
        self.memory_limit = memory_limit
        self.next_request_file_offset = initial_file_offset
        self.total_chunks = chunks

        self.current_chunk_index = 0

    def next_request(self) -> Optional[Request]:
        if self.current_chunk_index >= len(self.total_chunks):
            return None

        request = Request(self.next_request_file_offset, [])

        current_request_memory_size = 0
        while self.current_chunk_index < len(self.total_chunks):
            candidate_chunk = self.total_chunks[self.current_chunk_index]
            if current_request_memory_size + candidate_chunk > self.memory_limit:
                break

            request.chunks.append(candidate_chunk)
            current_request_memory_size = current_request_memory_size + candidate_chunk
            self.next_request_file_offset = (
                self.next_request_file_offset + candidate_chunk
            )
            self.current_chunk_index = self.current_chunk_index + 1

        return request

    @staticmethod
    def with_memory_cap(
        memory_mode: MemoryCapMode,
        initial_offset: int,
        chunks: List[int],
        user_memory_limit: Optional[int] = None,
    ) -> Tuple[RequestsIterator, int]:
        memory_limit = 0
        if memory_mode == MemoryCapMode.unlimited:
            memory_limit = sum(chunks)
        elif memory_mode == MemoryCapMode.largest_chunk:
            memory_limit = max(chunks)
        elif memory_mode == MemoryCapMode.limited:
            if user_memory_limit is None:
                raise RunaiStreamerMemoryLimitException(
                    f"MemoryCapMode is Limited, but no limit supplied"
                )
            largest_chunk = max(chunks)
            if user_memory_limit < largest_chunk:
                raise RunaiStreamerMemoryLimitException(
                    f"Memory limit supplied: {user_memory_limit} cannot be smaller than: {largest_chunk}"
                )
            memory_limit = user_memory_limit
        return RequestsIterator(memory_limit, initial_offset, chunks), memory_limit

    @staticmethod
    def with_memory_mode(
        initial_offset: int, chunks: List[int]
    ) -> Tuple[RequestsIterator, int]:
        memory_mode = _get_memory_mode()
        memory_limit = os.getenv(RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME)
        if memory_limit is not None:
            memory_limit = int(memory_limit)
        return RequestsIterator.with_memory_cap(
            memory_mode, initial_offset, chunks, memory_limit
        )


def _get_memory_mode() -> MemoryCapMode:
    memory_limit = os.getenv(RUNAI_STREAMER_MEMORY_LIMIT_ENV_VAR_NAME)
    if memory_limit is None or memory_limit == "-1":
        return MemoryCapMode.unlimited
    elif memory_limit == "0":
        return MemoryCapMode.largest_chunk
    else:
        return MemoryCapMode.limited
