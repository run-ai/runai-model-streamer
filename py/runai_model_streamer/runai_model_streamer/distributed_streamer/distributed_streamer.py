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

from runai_model_streamer.distributed_streamer.partition import (
    partition,
    create_broadcast_plan
)

import humanize
from timeit import default_timer as timer

class DistributedStreamer:
    def __init__(self) -> None:
        self.file_streamer = FileStreamer()
        self.device_str = None
        self.is_distributed = False
        self.partitions = {} # partitions of all the input file_stream_requests
        self.rank_file_chunks_list = {} # post Partitioning FileChunks to be streamed by this rank
        self.rank_dicts_map = {} # maps post partitioning FileChunks.id and chunk index to the original FileChunks.id and chunk index and chunk size
        self.broadcast_plan = {}
        self.is_distributed = False
        self.rank = 0

    def __enter__(self) -> DistributedStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def get_group_size(self) -> int:
        if not torch.distributed.is_initialized():
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
        self.is_distributed = torch.distributed.is_initialized()
        self.is_distributed = self.is_distributed and self.get_group_size() > 1
        if self.is_distributed:
            self.rank = dist.get_rank()
        else:
            self.rank = 0

        if self.is_distributed:
            if self.device_str == "cpu" and dist.get_backend() == "nccl":
                raise ValueError("Distributed backend nccl does not support cpu tensors")

        if not self.is_distributed:
            self.file_streamer.stream_files(file_stream_requests, credentials, device)
            return
        
        # partition tensors between processes
        self.partitions = partition(file_stream_requests, self.get_group_size())
        
        self.broadcast_plan = create_broadcast_plan(self.partitions)

        # read partition
        self.rank_file_chunks_list = []
        self.rank_dicts_map = {}
        for fc, d in self.partitions[self.rank]:
            self.rank_file_chunks_list.append(fc)
            self.rank_dicts_map[fc.id] = d
        if len(self.rank_file_chunks_list) > 0:
            print(f"rank {self.rank} has {len(self.rank_file_chunks_list)} files to stream to device {self.device_str}")
            self.file_streamer.stream_files(self.rank_file_chunks_list, credentials, self.device_str)
        else:
            print(f"rank {self.rank} has no files to stream")
 
    def get_chunks(self) -> Iterator:
        if not self.file_streamer:
            raise ValueError("Streamer not initialized")
        
        if not self.is_distributed:
            for file_path, ready_chunk_index, buffer in self.file_streamer.get_chunks():
                yield file_path, ready_chunk_index, buffer
            return

        # wait for all processes to reach this point
        dist.barrier()
        
        # allocate metadata buffer for three integers according to the device type
        metadata_buffer = torch.zeros(3, dtype=torch.int64, device=self.device_type)

        # broadcast and wait for ready tensors
        # TODO (Noa) : Handle exceptions by the file streamer, broadcast error code in case of a failure to obtain the chunk, broadcast with timeout in case a process terminates to avoid hanging processes

        ready_chunks_iterator = self.file_streamer.get_chunks()

        total_wait_time = 0
        total_broadcast_time = 0

        for i in range(len(self.broadcast_plan)):
            if self.broadcast_plan[i] == self.rank:
                # wait for the next chunk
        
                try:
                    ready_request_index, ready_chunk_index, buffer = next(ready_chunks_iterator)
                except ValueError as e:    
                    return

                # translate chunk index to original request index, chunk index and size
                original_request_index, original_chunk_index, original_chunk_size = self.rank_dicts_map[ready_request_index][ready_chunk_index]

                start_time = timer()
                # broadcast chunk identifiers
                metadata_buffer[0] = original_request_index
                metadata_buffer[1] = original_chunk_index
                metadata_buffer[2] = original_chunk_size
                dist.broadcast(metadata_buffer, self.rank)
                # broadcast data
                buffer.to(self.device_type)
                dist.broadcast(buffer, self.rank)
                end_time = timer()
                total_broadcast_time += end_time - start_time
                print(f"brodcasted buffer after waiting {end_time - start_time} seconds - rank {self.rank} -  original_request_index {original_request_index} original_chunk_index {original_chunk_index} original_chunk_size {original_chunk_size}", flush=True)
                yield original_request_index, original_chunk_index, buffer
            else:
                # wait for broadcast chunk identifiers
                start_time = timer()
                metadata_buffer.to(self.device_type)
                dist.broadcast(metadata_buffer, self.broadcast_plan[i])
                # Use .item() to get scalar values without a full tensor copy
                original_request_index = metadata_buffer[0].item()
                original_chunk_index = metadata_buffer[1].item()
                original_chunk_size = metadata_buffer[2].item()

                # allocate buffer for the chunk on the correct device
                # TODO (Noa) : allocate a buffer in the size of the largest chunk and reuse it instead of allocating a new one each time
                buffer = torch.empty(original_chunk_size, dtype=torch.uint8, device=self.device_type)

                # wait for broadcast data
                dist.broadcast(buffer, self.broadcast_plan[i])

                end_time = timer()
                total_wait_time += end_time - start_time
                print(f"got buffer after waiting {end_time - start_time} seconds - rank {self.rank} -  original_request_index {original_request_index} original_chunk_index {original_chunk_index} original_chunk_size {original_chunk_size}", flush=True)
                yield original_request_index, original_chunk_index, buffer
        print(f"******************* rank {self.rank} total wait time: {total_wait_time} seconds ; total broadcast time: {total_broadcast_time} seconds")
