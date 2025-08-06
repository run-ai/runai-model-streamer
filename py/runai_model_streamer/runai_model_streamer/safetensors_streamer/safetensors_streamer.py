from __future__ import annotations
from typing import Iterator, Optional
import torch
import torch.distributed as dist
import glob
import os
from typing import List

from runai_model_streamer.file_streamer import FileChunks

from runai_model_streamer.distributed_streamer import DistributedStreamer

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
        self.file_streamer = DistributedStreamer()
        self.files_to_tensors_metadata = {}
        self.device_str = None

    def __enter__(self) -> SafetensorsStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def stream_file(
            self,
            path: str,
            s3_credentials : Optional[S3Credentials] = None,
            device: Optional[str] = None,
        ) -> None:
        return self.stream_files([path], s3_credentials, device)

    # TODO (Noa) to be removed after testing
    # device type should be handled by the caller
    def get_device_str(self, device_type: Optional[str] = None) -> str:
        if dist.is_initialized():
            backend_name = dist.get_backend()
            if backend_name == "nccl":
                # TODO (Noa) vllm does not set LOCAL_RANK - should be checked if this is the local or global rank
                rank = dist.get_rank()
                return f"cuda:{rank}"
            else:
                return "cpu"
        else:
            if device_type is None:
                return "cpu"
            return device_type
        # if device_type == torch.device("cpu"):
        #     return "cpu"
        # else:
        #     local_rank = int(os.getenv("LOCAL_RANK"))
        #     return f"cuda:{local_rank}"
    
    def stream_files(
            self,
            paths: List[str],
            s3_credentials : Optional[S3Credentials] = None,
            device: Optional[str] = None,
        ) -> None:
        self.files_to_tensors_metadata = {}

        # TODO (Noa) to be removed after testing and vllm integration
        # TODO (Noa)sending device type cpu while torch distributed backend is nccl will crash and should be checked

        self.device_str = self.get_device_str(device)

        file_stream_requests: List[FileChunks] = []

        safetensors_metadatas = safetensors_pytorch.prepare_request(self.file_streamer, paths, s3_credentials, self.device_str)

        for i in range(len(paths)):
            (file_offset, tensors_metadata, tensor_sizes) = safetensors_metadatas[i]
            path = paths[i]
            self.files_to_tensors_metadata[i] = tensors_metadata
            file_stream_requests.append(FileChunks(i, path, file_offset, tensor_sizes))

        self.file_streamer.stream_files(
            file_stream_requests,
            credentials=s3_credentials,
            device=self.device_str
        )

    def get_tensors(self) -> Iterator[torch.tensor]:
        for file_index, ready_chunk_index, buffer in self.file_streamer.get_chunks():
            tensor_metadata = self.files_to_tensors_metadata[file_index][ready_chunk_index]
            yield tensor_metadata.name, safetensors_pytorch.create_torch_tensor(
                buffer, tensor_metadata
            )
