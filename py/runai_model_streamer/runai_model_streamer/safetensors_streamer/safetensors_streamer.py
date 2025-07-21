from __future__ import annotations
from typing import Iterator, Optional
import torch
import glob
import os
from typing import List
from runai_model_streamer.file_streamer import (
    FileStreamer,
    FileChunks
)
import runai_model_streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    s3_glob,
    s3_pull_files,
)

SAFETENSORS_PATTERN = "*.safetensors"

def list_safetensors(path: str, s3_credentials : Optional[S3Credentials] = None) -> List[str]:
    if is_s3_path(path):
        return s3_glob(path, [SAFETENSORS_PATTERN], s3_credentials)
    return glob.glob(os.path.join(path, SAFETENSORS_PATTERN))

def pull_files(model_path: str,
                dst: str,
                allow_pattern: Optional[List[str]] = None,
                ignore_pattern: Optional[List[str]] = None,
                s3_credentials : Optional[S3Credentials] = None) -> None:
    if is_s3_path(model_path):
        return s3_pull_files(model_path, dst, allow_pattern, ignore_pattern, s3_credentials)
    raise NotImplementedError("pull files is not implemented for file system paths")

class SafetensorsStreamer:
    def __init__(self) -> None:
        self.file_streamer = FileStreamer()
        self.files_to_tensors_metadata = {}

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
        return self.stream_files([path], s3_credentials)
    
    def stream_files(
            self,
            paths: List[str],
            s3_credentials : Optional[S3Credentials] = None,
        ) -> None:
        self.files_to_tensors_metadata = {}

        file_stream_requests: List[FileChunks] = []

        safetensors_metadatas = safetensors_pytorch.prepare_request(self.file_streamer, paths)
        for i in range(len(paths)):
            (file_offset, tensors_metadata, tensor_sizes) = safetensors_metadatas[i]
            path = paths[i]
            self.files_to_tensors_metadata[path] = tensors_metadata
            file_stream_requests.append(FileChunks(path, file_offset, tensor_sizes))

        self.file_streamer.stream_files(
            file_stream_requests,
            s3_credentials,
        )

    def get_tensors(self) -> Iterator[torch.tensor]:
        for file_path, ready_chunk_index, buffer in self.file_streamer.get_chunks():
            tensor_metadata = self.files_to_tensors_metadata[file_path][ready_chunk_index]
            yield tensor_metadata.name, safetensors_pytorch.create_torch_tensor(
                buffer, tensor_metadata
            )
