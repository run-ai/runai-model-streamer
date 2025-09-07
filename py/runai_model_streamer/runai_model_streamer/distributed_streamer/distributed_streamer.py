from __future__ import annotations
from typing import List, Iterator, Optional
from typing import Iterator, Optional, List
import torch
import torch.distributed as dist
import glob
import os
from datetime import timedelta
import socket
import re

from runai_model_streamer.file_streamer import (
    FileStreamer,
    FileChunks,
)

from runai_model_streamer.distributed_streamer.partition import (
    partition,
    get_total_number_of_chunks,
    get_partition_policy
)

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    is_gs_path,
)

import humanize
from timeit import default_timer as timer

DEFAULT_BROADCAST_TIMEOUT = timedelta(seconds=600)

class DistributedStreamer:
    def __init__(self) -> None:
        self.file_streamer = FileStreamer()
        self.device_str = None
        self.is_distributed = False
        self.partitions = {} # partitions of all the input file_stream_requests
        self.rank_file_chunks_list = {} # post Partitioning FileChunks to be streamed by this rank
        self.rank_dicts_map = {} # maps post partitioning FileChunks.id and chunk index to the original FileChunks.id and chunk index and chunk size
        self.total_chunks_to_read = 0
        self.rank = 0
        self.original_group_rank = 0
        self.distribution_group = None
        self.reading_from_storage = False
        self.local_group_global_ranks = []
        self.is_error = False

    def __enter__(self) -> DistributedStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        if self.distribution_group and not self.is_error:
            torch.distributed.barrier(group=self.distribution_group)
            torch.distributed.destroy_process_group(group=self.distribution_group)
            del self.distribution_group
            self.distribution_group = None
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def get_group_size(self) -> int:
        if not dist.is_initialized():
            return 1
        return dist.get_world_size()

    def set_is_distributed(self, path: str, device: Optional[str] = None) -> None:
        # check if distributed streaming should be used

        # by default, use distributed streaming only for object storage paths
        if path is not None:
            self.is_distributed = is_s3_path(path) or is_gs_path(path)

        # environment variable to override default distributed streaming
        if os.environ.get("RUNAI_STREAMER_DIST") == "0":
            print(f"RUNAI_STREAMER_DIST is set to 0")
            self.is_distributed = False
        elif os.environ.get("RUNAI_STREAMER_DIST") == "1":
            print(f"RUNAI_STREAMER_DIST is set to 1")
            self.is_distributed = True

        # check if torch distributed is initialized and there are more than one process
        if self.is_distributed:
            self.is_distributed = dist.is_initialized()
            self.is_distributed = self.is_distributed and self.get_group_size() > 1

        self.set_device_str(device)
        self.device_type = torch.device(self.device_str)

        # do not distribute if backend type does not match device type
        if self.is_distributed:
            backend_name = dist.get_backend()
            if backend_name == "nccl" and self.device_str == "cpu":
                self.is_distributed = False
            if backend_name == "gloo" and self.device_str != "cpu":
                self.is_distributed = False

            if backend_name != "nccl" and backend_name != "gloo":
                print(f"DistributedStreamer: backend {backend_name} is not supported - using non-distributed mode")
                self.is_distributed = False

        # check if there is enough free memory on the device
        if self.is_distributed and torch.cuda.is_available() and self.device_str != "cpu":
            free_memory = self.get_cuda_free_memory()
            if free_memory < 2 * self.max_chunk:
                print(f"Warning: Not enough memory on the device for distributed streaming, free memory: {free_memory} bytes, required minimun: {2 * self.max_chunk} bytes")
                self.is_distributed = False

    def get_broadcast_timeout(self) -> timedelta:
        timeout_val = os.environ.get("RUNAI_STREAMER_DIST_TIMEOUT")
        if timeout_val:
            return timedelta(seconds=int(timeout_val))
        else:
            return DEFAULT_BROADCAST_TIMEOUT

    def set_device_str(self, device: Optional[str] = None) -> None:
        # cpu specified
        if device is "cpu":
            self.device_str = device
            return

        # specific cuda device
        if device is not None:
            cuda_pattern = re.compile(r"^cuda:\d+$", re.IGNORECASE)
            if cuda_pattern.match(device) is not None:
                self.device_str = device
            return

        # cuda or None
        if self.is_distributed:
            backend_name = dist.get_backend()
            if backend_name == "nccl":
                device = torch.cuda.current_device()
                self.device_str = f"cuda:{device}"
            else:
                self.device_str = "cpu"
        else:
            self.device_str = "cpu"

    def create_distribution_group(self) -> dist.GroupSpec:
        if self.distribution_group:
            return self.distribution_group

        if not self.is_distributed:
            return None

        is_global_group = int(os.environ.get("RUNAI_STREAMER_DIST_GLOBAL", "0")) == 1

        if is_global_group:
            # create global group
            world_size = self.get_group_size()

            all_ranks = list(range(world_size))
            group_timeout = self.get_broadcast_timeout()
            self.local_group_global_ranks = all_ranks
            group = dist.new_group(ranks = all_ranks, timeout = group_timeout)

        else:
            group = self.create_local_distribution_group()
        return group

    def create_local_distribution_group(self) -> dist.GroupSpec:
        """
        Creates a torch.distributed.ProcessGroup containing all ranks on the current node.
        This version uses a coordinated creation pattern to avoid deadlocks.

        This function performs all_gather_object on the global group which cause nccl to allocate  device memory which otherwise may never be allocated by the user application
        To avoid this issue, we create a another global subgroup just for discovering the local ranks and then destroy that subgroup after the ranks are discovered.
        """
        if not dist.is_initialized():
            return None

        group_timeout = self.get_broadcast_timeout()
        my_global_rank = dist.get_rank()
        world_size = dist.get_world_size()

        # 1. Discover all peers on all nodes (this is a global collective)
        # create global group just for discovering the local ranks
        sandbox_global_group = dist.new_group(ranks=list(range(world_size)), timeout=group_timeout)
        all_hostnames = [None] * world_size
        dist.all_gather_object(all_hostnames, socket.gethostname(), group=sandbox_global_group)
        dist.destroy_process_group(group=sandbox_global_group)
        del sandbox_global_group

        # 2. Create a list of rank lists, one for each unique host.
        # e.g., [[0,1,2,3,4,5,6,7], [8,9,10,11,12,13,14,15]]
        unique_hostnames = sorted(list(set(all_hostnames)))
        groups_by_ranks = []
        for hostname in unique_hostnames:
            ranks_on_host = [r for r, h in enumerate(all_hostnames) if h == hostname]
            groups_by_ranks.append(ranks_on_host)

        # 3. All processes must create ALL subgroups in the same order.
        # This is a global collective operation, done once for each subgroup.
         
        created_groups = [dist.new_group(ranks=ranks, timeout=group_timeout) for ranks in groups_by_ranks]
        
        # 4. Now, each process finds which of the newly created groups it belongs to.
        my_local_group = None
        for i, ranks_list in enumerate(groups_by_ranks):
            if my_global_rank in ranks_list:
                my_local_group = created_groups[i]
                self.local_group_global_ranks = ranks_list # Save the mapping
                break

        return my_local_group

    def set_rank(self) -> None:
        if self.is_distributed:
            self.rank = dist.get_rank(group=self.distribution_group)
            self.original_group_rank = dist.get_rank()

    def get_cuda_free_memory(self) -> int:
        if not torch.cuda.is_available():
            return 0
        free_memory, total_memory = torch.cuda.mem_get_info()
        return free_memory

    def stream_files(
            self,
            file_stream_requests: List[FileChunks],
            credentials: Optional[S3Credentials] = None,
            device: Optional[str] = None,
    ) -> None:

        # find the size of the maximal chunk for the reusable buffer
        max_chunks_per_file = (fc.max_chunk_size() for fc in file_stream_requests if fc.chunks)
        self.max_chunk = max(max_chunks_per_file, default=0)

        path = None
        if len(file_stream_requests) > 0:
            path = file_stream_requests[0].path

        # check if distributed streaming can be used
        self.set_is_distributed(path, device)

        print(f"is distributed: {self.is_distributed} device: {self.device_str}")

        if not self.is_distributed:
            self.file_streamer.stream_files(file_stream_requests, credentials, device)
            return

        # check if distributed backend supports cpu tensors
        if self.device_str == "cpu" and dist.get_backend() == "nccl":
            raise ValueError("Distributed backend nccl does not support cpu tensors")

        # create distribution group for configuring timeout and possibly broadcast locally in each node 
        # The group must be created before partitioning the tensors, in case the group will be local
        self.distribution_group = self.create_distribution_group()


        # set rank in the new distribution group
        self.set_rank()

        # partition tensors between processes in the distribution group
        self.partitions = partition(file_stream_requests, dist.get_world_size(group=self.distribution_group))

        self.total_chunks_to_read = get_total_number_of_chunks(self.partitions)
        
        # read partition
        self.rank_file_chunks_list = []

        # map partitions's file chunks index to the index in the original file list
        self.rank_dicts_map = {}
        for fc, d in self.partitions[self.rank]:
            self.rank_file_chunks_list.append(fc)
            self.rank_dicts_map[fc.id] = d

        if len(self.rank_file_chunks_list) == 0:
            self.reading_from_storage = False
            return

        # read files
        original_memory_limit = os.environ.get("RUNAI_STREAMER_MEMORY_LIMIT")
        try:
            # for distributed streaming only - change default memory limit to unlimited
            if original_memory_limit == None:
                os.environ["RUNAI_STREAMER_MEMORY_LIMIT"] = "-1"
            self.file_streamer.stream_files(self.rank_file_chunks_list, credentials, "cpu")
        except Exception as e:
            raise e
        finally:
            if original_memory_limit is None:
                os.environ.pop("RUNAI_STREAMER_MEMORY_LIMIT", None)
            else:
                os.environ["RUNAI_STREAMER_MEMORY_LIMIT"] = original_memory_limit
        self.reading_from_storage = True

    def get_chunks(self) -> Iterator:
        if not self.file_streamer:
            raise ValueError("Streamer not initialized")
        
        if not self.is_distributed:
            #f"rank {self.rank} streaming with no distribution")
            for item in self.file_streamer.get_chunks():
                yield item
            return
     
        start_time = timer()

        MAX_CHUNKS_PER_BATCH = 256
        DEFAULT_BUFFER_MIN_BYTESIZE = 1024 * 1024 * 1024
        BUFFER_MIN_BYTESIZE = int(os.environ.get("RUNAI_STREAMER_DIST_BUFFER_MIN_BYTESIZE", str(DEFAULT_BUFFER_MIN_BYTESIZE))) # environment variable used for testing
        BUFFER_BYTESIZE = max(BUFFER_MIN_BYTESIZE, self.max_chunk)

        data_buffer = torch.empty(BUFFER_BYTESIZE, dtype=torch.uint8, device=self.device_type)
        received_buffer = torch.empty(BUFFER_BYTESIZE, dtype=torch.uint8, device=self.device_type)

        batch_metadata_tensor = torch.zeros(MAX_CHUNKS_PER_BATCH + 1, 4, dtype=torch.int64, device=self.device_type)
        received_metadata_tensor = torch.zeros(MAX_CHUNKS_PER_BATCH + 1, 4, dtype=torch.int64, device=self.device_type)
        
        ready_chunks_iterator = self.file_streamer.get_chunks()
        
        def chunk_generator():
            yield from ready_chunks_iterator
            while True:
                yield None

        chunks_to_read = [self.total_chunks_to_read]
        chunk_gen = chunk_generator()
        leftover_chunk = [None]

        try:
            while chunks_to_read[0] > 0:
                # --- Fill staging buffer ---
                current_data_size, chunk_count_in_batch = self.prefill(
                    chunk_gen,
                    MAX_CHUNKS_PER_BATCH,
                    BUFFER_BYTESIZE,
                    data_buffer,
                    received_buffer,
                    batch_metadata_tensor,
                    received_metadata_tensor,
                    leftover_chunk)

                # --- Broadcast ---
                yield from self.broadcast(
                    chunks_to_read,
                    batch_metadata_tensor,
                    received_metadata_tensor,
                    data_buffer,
                    received_buffer,
                    chunk_count_in_batch,
                    current_data_size)
                
        except RuntimeError as e:
            # Check if the error is a timeout
            self.is_error = True
            if "timed out" in str(e).lower() or "timeout" in str(e).lower():
                print(f"Rank {self.original_group_rank}: broadcast timed out - Could not complete broadcast.")
            else:
                print(f"rank {self.original_group_rank} error: {e}")
            raise e
        except Exception as e:
            self.is_error = True
            print(f"rank {self.original_group_rank} error: {e}")
            raise e
        finally:
            end_time = timer()
            print(f"local rank {self.rank} global rank {self.original_group_rank} {'failed' if self.is_error else 'done'} in {end_time - start_time} seconds")           

    def prefill(self,
                chunk_gen: Iterator,
                max_chunks_per_batch: int,
                buffer_bytesize: int,
                data_buffer: torch.Tensor,
                received_buffer: torch.Tensor,
                batch_metadata_tensor: torch.Tensor,
                received_metadata_tensor: torch.Tensor,
                leftover_chunk: List[Tuple[int, int, torch.Tensor]]) -> Tuple[int, int, bool]:

        current_data_size = 0
        chunk_count_in_batch = 0

        while chunk_count_in_batch < max_chunks_per_batch and self.reading_from_storage:

            # Prioritize the leftover chunk from the previous batch.
            if leftover_chunk[0]:
                chunk_item = leftover_chunk[0]
                leftover_chunk[0] = None
            else:
                chunk_item = next(chunk_gen)

            if chunk_item is None:
                break

            ready_request_id, ready_chunk_index, cpu_buffer = chunk_item
            chunk_size = cpu_buffer.numel()

            if current_data_size + chunk_size > buffer_bytesize:
                # This chunk doesn't fit, so we save it for the next batch.
                leftover_chunk[0] = chunk_item
                break 

            data_buffer[current_data_size : current_data_size + chunk_size].copy_(cpu_buffer.squeeze())
            
            orig_req_idx, orig_chunk_idx, _ = self.rank_dicts_map[ready_request_id][ready_chunk_index]
            batch_metadata_tensor[chunk_count_in_batch + 1].copy_(
                torch.tensor([orig_req_idx, orig_chunk_idx, chunk_size, current_data_size])
            )
            
            current_data_size += chunk_size
            chunk_count_in_batch += 1 

        batch_metadata_tensor[0, 0] = chunk_count_in_batch

        return current_data_size, chunk_count_in_batch

    def broadcast(self,
                  chunks_to_read: List[int],
                  batch_metadata_tensor: torch.Tensor,
                  received_metadata_tensor: torch.Tensor,
                  data_buffer: torch.Tensor,
                  received_buffer: torch.Tensor,
                  chunk_count_in_batch: int,
                  current_data_size: int) -> Iterator:        

        group_size = dist.get_world_size(group=self.distribution_group)

        total_broadcast_chunks = 0

        for broadcasting_rank in range(group_size):
            # Map the local rank to its corresponding GLOBAL rank
            global_broadcasting_rank = self.local_group_global_ranks[broadcasting_rank]

            # self.original_group_rank is the GLOBAL rank of the current process
            if global_broadcasting_rank == self.original_group_rank:
                chunks_to_read[0] -= chunk_count_in_batch
                total_broadcast_chunks += chunk_count_in_batch
                # broadcast metadata
                dist.broadcast(batch_metadata_tensor, global_broadcasting_rank, group=self.distribution_group)

                # broadcast data
                if chunk_count_in_batch > 0:
                    dist.broadcast(data_buffer[:current_data_size], global_broadcasting_rank, group=self.distribution_group)

                # yield
                    for j in range(chunk_count_in_batch):
                        meta = batch_metadata_tensor[j + 1]
                        offset, size = meta[3].item(), meta[2].item()
                        yield meta[0].item(), meta[1].item(), data_buffer[offset : offset + size]
            else:
                # receive metadata
                dist.broadcast(received_metadata_tensor, global_broadcasting_rank, group=self.distribution_group)

                received_chunk_count_in_batch = received_metadata_tensor[0, 0].item()

                if received_chunk_count_in_batch == 0:
                    continue

                total_broadcast_chunks += received_chunk_count_in_batch

                # receive data
                last_meta = received_metadata_tensor[received_chunk_count_in_batch]
                total_data_size = last_meta[3].item() + last_meta[2].item() # offset plus size of last chunk in batch

                chunks_to_read[0] -= received_chunk_count_in_batch

                received_data_buf_view = received_buffer[:total_data_size]

                dist.broadcast(received_data_buf_view, global_broadcasting_rank, group=self.distribution_group)

                for j in range(received_chunk_count_in_batch):
                    meta = received_metadata_tensor[j + 1]
                    offset, size = meta[3].item(), meta[2].item()
                    yield meta[0].item(), meta[1].item(), received_data_buf_view[offset : offset + size]

        if total_broadcast_chunks == 0 and chunks_to_read[0] > 0:
            print(f"Error: rank {self.rank} is missing {chunks_to_read[0]} chunks", flush=True)
            raise RuntimeError("No chunks to read")
