from __future__ import annotations
from typing import List, Iterator, Optional
from typing import Iterator, Optional
import torch
import torch.distributed as dist
import glob
import os
from typing import List

from runai_model_streamer.s3_utils.s3_utils import S3Credentials

from runai_model_streamer.file_streamer import (
    FileStreamer,
    FileChunks,
)

import humanize
from timeit import default_timer as timer

class DistributedStreamer:
    def __init__(self) -> None:
        self.file_streamer = FileStreamer()
        self.start_time = timer()
        self.total_size = 0
        self.device_str = None
        self.is_distributed = False

    def __enter__(self) -> DistributedStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        if self.device_str is not None:
            size = self.total_size
            elapsed_time = timer() - self.start_time
            throughput = size / elapsed_time
            print(
                f"[RunAI Distributed Streamer] Overall time to stream {humanize.naturalsize(size, binary=True)} of all files to {self.device_str}: {round(elapsed_time, 2)}s, {humanize.naturalsize(throughput, binary=True)}/s",
                flush=True,
            )

    def get_group_size(self) -> int:
        if not self.is_distributed:
            return 1
        return dist.get_world_size()

    def stream_files(
            self,
            file_stream_requests: List[FileChunks],
            credentials: Optional[S3Credentials] = None,
            device: Optional[str] = None,
) -> None:
        if device is None:
            self.device_str = "cpu"
        else:
            self.device_str = device
        self.device_type = torch.device(self.device_str)

        # check if distributed
        self.is_distributed = torch.distributed.is_initialized() and get_group_size() > 1

        if not self.is_distributed:
            self.file_streamer.stream_files(file_stream_requests, credentials, device)
            return
        
        # partition tensors between processes

        # read partition 

 
    def get_chunks(self) -> Iterator:
        if not self.file_streamer:
            raise ValueError("Streamer not initialized")
        
        if not self.is_distributed:
            for file_path, ready_chunk_index, buffer in self.file_streamer.get_chunks():
                yield file_path, ready_chunk_index, buffer
            return
        
        # broadcast and wait for ready tensors
        
