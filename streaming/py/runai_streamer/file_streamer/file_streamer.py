from typing import List, Iterator
from runai_streamer.libstreamer.libstreamer import (
    runai_start,
    runai_end,
    runai_read,
    runai_request,
    runai_response,
)


class FileStreamer:
    def __enter__(self) -> "FileStreamer":
        self.streamer = runai_start()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        if self.streamer:
            runai_end(self.streamer)

    def read_file(self, path: str, offset: int, dst: memoryview) -> None:
        runai_read(self.streamer, path, offset, len(dst), dst)

    def stream_file(
        self, path: str, offset: int, dst: memoryview, internal_sizes: List[int]
    ) -> None:
        self.internal_sizes = internal_sizes
        runai_request(
            self.streamer,
            path,
            offset,
            len(dst),
            dst,
            self.internal_sizes,
        )

    def get_chunks(self) -> Iterator:
        if not self.streamer:
            raise ValueError("Streamer not initialized")
        for _ in range(len(self.internal_sizes)):
            index = runai_response(self.streamer)
            if index == None:
                return
            yield index, sum(self.internal_sizes[:index])
