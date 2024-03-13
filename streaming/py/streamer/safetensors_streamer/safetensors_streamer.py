from __future__ import annotations
from typing import Iterator
import torch
from streamer.file_streamer.file_streamer import FileStreamer
import streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch


class SafetensorsStreamer:
    def __init__(self) -> None:
        self.file_streamer = FileStreamer()

    def __enter__(self) -> SafetensorsStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def stream_file(self, path: str) -> None:
        offset, self.tensors_metadata, tensor_sizes = (
            safetensors_pytorch.prepare_request(path)
        )
        self.dst = bytearray(sum(tensor_sizes))
        self.file_streamer.stream_file(path, offset, self.dst, tensor_sizes)

    def get_tensors(self) -> Iterator[torch.tensor]:
        for index, offset in self.file_streamer.get_chunks():
            tensor_metadata = self.tensors_metadata[index]
            yield tensor_metadata.name, safetensors_pytorch.create_torch_tensor(
                self.dst, offset, tensor_metadata
            )
