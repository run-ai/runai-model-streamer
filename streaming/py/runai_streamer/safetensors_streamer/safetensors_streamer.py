from __future__ import annotations
from typing import Iterator
import torch
import mmap
import os
from runai_streamer.file_streamer.file_streamer import FileStreamer
import runai_streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch

RUNAI_DIRNAME = "RUNAI_DIRNAME"
RUNAI_DIRNAME_TO_REMOVE = "RUNAI_DIRNAME_TO_REMOVE"


def convert_path_if_needed(path: str) -> str:
    s3_dir = os.getenv(RUNAI_DIRNAME)
    if s3_dir is None:
        return path
    dir_to_remove = os.getenv(RUNAI_DIRNAME_TO_REMOVE)
    if dir_to_remove is None:
        return os.path.join(s3_dir, os.path.basename(path))
    return os.path.join(s3_dir, os.path.relpath(path, dir_to_remove))


class SafetensorsStreamer:
    def __init__(self) -> None:
        self.file_streamer = FileStreamer()

    def __enter__(self) -> SafetensorsStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def stream_file(self, path: str) -> None:
        path = convert_path_if_needed(path)

        offset, self.tensors_metadata, tensor_sizes = (
            safetensors_pytorch.prepare_request(self.file_streamer, path)
        )
        self.dst = mmap.mmap(
            -1, sum(tensor_sizes), mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE
        )
        self.file_streamer.stream_file(path, offset, self.dst, tensor_sizes)

    def get_tensors(self) -> Iterator[torch.tensor]:
        for index, offset in self.file_streamer.get_chunks():
            tensor_metadata = self.tensors_metadata[index]
            yield tensor_metadata.name, safetensors_pytorch.create_torch_tensor(
                self.dst, offset, tensor_metadata
            )
