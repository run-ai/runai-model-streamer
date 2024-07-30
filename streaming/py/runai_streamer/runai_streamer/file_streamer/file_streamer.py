from typing import List, Iterator
from timeit import default_timer as timer
from runai_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_end,
    runai_read,
    runai_request,
    runai_response,
)
import humanize
import mmap


class FileStreamer:
    def __enter__(self) -> "FileStreamer":
        self.streamer = runai_start()
        self.start_time = timer()
        self.total_size = 0
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

    def read_file(self, path: str, offset: int, len: int) -> memoryview:
        dst_buffer = mmap.mmap(-1, len, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE)
        runai_read(self.streamer, path, offset, len, dst_buffer)
        return dst_buffer

    def stream_file(self, path: str, file_offset: int, chunks: List[int]) -> None:
        request_size = sum(chunks)
        self.total_chunks = chunks
        self.total_size = self.total_size + request_size

        self.dst_buffer = mmap.mmap(
            -1, request_size, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE
        )

        runai_request(
            self.streamer,
            path,
            file_offset,
            len(self.dst_buffer),
            self.dst_buffer,
            self.total_chunks,
        )

    def get_chunks(self) -> Iterator:
        if not self.streamer:
            raise ValueError("Streamer not initialized")
        for _ in range(len(self.total_chunks)):
            ready_chunk_index = runai_response(self.streamer)
            if ready_chunk_index == None:
                return
            yield ready_chunk_index, self.dst_buffer, sum(
                self.total_chunks[:ready_chunk_index]
            )
