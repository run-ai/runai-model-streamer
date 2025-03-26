from __future__ import annotations
from typing import Iterator, Optional
import torch
import os
from runai_model_streamer.file_streamer.file_streamer import FileStreamer
import runai_model_streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
)

class SafetensorsStreamer:
    def __init__(self) -> None:
        self.file_streamer = FileStreamer()

    def __enter__(self) -> SafetensorsStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def stream_file(
            self,
            path: str,
            s3_credentials : Optional[S3Credentials] = None,
        ) -> None:

        file_offset, self.tensors_metadata, tensor_sizes = (
            safetensors_pytorch.prepare_request(self.file_streamer, path)
        )
        self.file_streamer.stream_file(
            path,
            file_offset,
            tensor_sizes,
            s3_credentials,
        )

    def get_tensors(self) -> Iterator[torch.tensor]:
        for ready_chunk_index, buffer, buffer_offset in self.file_streamer.get_chunks():
            tensor_metadata = self.tensors_metadata[ready_chunk_index]
            yield tensor_metadata.name, safetensors_pytorch.create_torch_tensor(
                buffer, buffer_offset, tensor_metadata
            )
