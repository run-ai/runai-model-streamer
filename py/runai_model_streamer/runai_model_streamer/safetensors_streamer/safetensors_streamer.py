from __future__ import annotations
from typing import Iterator, Optional
import torch
import glob
import os
from typing import List

from runai_model_streamer.file_streamer import FileChunks

from runai_model_streamer.distributed_streamer import DistributedStreamer

import runai_model_streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    is_gs_path,
    is_azure_path,
    s3_glob,
    s3_pull_files,
    gcs_glob,
    gcs_pull_files,
    azure_glob,
    azure_pull_files,
)

SAFETENSORS_PATTERN = "*.safetensors"

def list_safetensors(path: str, s3_credentials : Optional[S3Credentials] = None) -> List[str]:
    """
    List all safetensors files in the given path.
    This function is not recursive.
    """
    if is_s3_path(path):
        files = s3_glob(path, [SAFETENSORS_PATTERN], s3_credentials)
    elif is_gs_path(path):
        files = gcs_glob(path, [SAFETENSORS_PATTERN])
    elif is_azure_path(path):
        files = azure_glob(path, [SAFETENSORS_PATTERN])
    else:
        files = glob.glob(os.path.join(path, SAFETENSORS_PATTERN))
    
    return files

def pull_files(model_path: str,
                dst: str,
                allow_pattern: Optional[List[str]] = None,
                ignore_pattern: Optional[List[str]] = None,
                s3_credentials : Optional[S3Credentials] = None) -> None:
    """
    Pull all safetensors files in the given path.
    This function is recursive.
    """
    if is_s3_path(model_path):
        return s3_pull_files(model_path, dst, allow_pattern, ignore_pattern, s3_credentials)
    if is_gs_path(model_path):
        return gcs_pull_files(model_path, dst, allow_pattern, ignore_pattern)
    if is_azure_path(model_path):
        return azure_pull_files(model_path, dst, allow_pattern, ignore_pattern)
    raise NotImplementedError("pull files is not implemented for file system paths")

class SafetensorsStreamer:
    def __init__(self) -> None:
        self.file_streamer = DistributedStreamer()
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
            device: Optional[str] = "cpu",
            is_distributed: bool = False,
        ) -> None:
        return self.stream_files([path], s3_credentials, device, is_distributed)

 
    def stream_files(
            self,
            paths: List[str],
            s3_credentials : Optional[S3Credentials] = None,
            device: Optional[str] = "cpu",
            is_distributed: bool = False, 
        ) -> None:
        self.files_to_tensors_metadata = {}

        file_stream_requests: List[FileChunks] = []

        # metadata is created on cpu and each process reads it individually
        safetensors_metadatas = safetensors_pytorch.prepare_request(self.file_streamer, paths, s3_credentials)

        for i in range(len(paths)):
            (file_offset, tensors_metadata, tensor_sizes) = safetensors_metadatas[i]
            path = paths[i]
            self.files_to_tensors_metadata[i] = tensors_metadata
            file_stream_requests.append(FileChunks(i, path, file_offset, tensor_sizes))

        self.file_streamer.stream_files(
            file_stream_requests,
            credentials=s3_credentials,
            device=device,
            is_distributed=is_distributed,
        )

    def get_tensors(self) -> Iterator[torch.tensor]:
        for file_index, ready_chunk_index, buffer in self.file_streamer.get_chunks():
            tensor_metadata = self.files_to_tensors_metadata[file_index][ready_chunk_index]
            yield tensor_metadata.name, safetensors_pytorch.create_torch_tensor(
                buffer, tensor_metadata
            )
