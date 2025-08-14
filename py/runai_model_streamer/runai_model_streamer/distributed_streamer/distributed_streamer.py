from __future__ import annotations
from typing import List, Iterator, Optional
from typing import Iterator, Optional, List
import torch
import torch.distributed as dist
import glob
import os

from runai_model_streamer.s3_utils.s3_utils import S3Credentials

from runai_model_streamer.file_streamer import (
    FileStreamer,
    FileChunks,
)

from runai_model_streamer.distributed_streamer.partition import (
    partition,
    create_broadcast_plan,
    get_partition_policy
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
        self.rank = 0
        self.reading_from_storage = False

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

        if os.environ.get("RUNAI_STREAMER_DIST") is not None:
            self.is_distributed = int(os.environ.get("RUNAI_STREAMER_DIST")) == 1

        print(f"rank {self.rank} is distributed: {self.is_distributed}")

        if self.is_distributed:
            self.rank = dist.get_rank()
        else:
            self.rank = 0

        if self.is_distributed:
            if self.device_str == "cpu" and dist.get_backend() == "nccl":
                raise ValueError("Distributed backend nccl does not support cpu tensors")

        if not self.is_distributed:
            print(f"rank {self.rank} streaming with no distribution")
            self.file_streamer.stream_files(file_stream_requests, credentials, device)
            return
        
        # partition tensors between processes
        self.partitions = partition(file_stream_requests, self.get_group_size())
        
        self.broadcast_plan = create_broadcast_plan(self.partitions)

        # find the size of the maximal chunk for the reusable buffer

        global_max_chunk = 0
        for p in self.partitions:
            for fc, _ in p:
                if fc.chunks:
                    global_max_chunk = max(global_max_chunk, max(fc.chunks))

        self.max_chunk = global_max_chunk

        # read partition
        self.rank_file_chunks_list = []
        self.rank_dicts_map = {}
        for fc, d in self.partitions[self.rank]:
            self.rank_file_chunks_list.append(fc)
            self.rank_dicts_map[fc.id] = d
        if len(self.rank_file_chunks_list) > 0:
            print(f"rank {self.rank} has {len(self.rank_file_chunks_list)} files to stream to device {self.device_str}")
            #self.file_streamer.stream_files(self.rank_file_chunks_list, credentials, self.device_str)
            self.file_streamer.stream_files(self.rank_file_chunks_list, credentials, "cpu")
            self.reading_from_storage = True
        else:
            self.reading_from_storage = False
            print(f"rank {self.rank} has no files to stream")    

    def get_chunks(self) -> Iterator:
        if not self.file_streamer:
            raise ValueError("Streamer not initialized")
        
        if not self.is_distributed:
            print(f"rank {self.rank} streaming with no distribution")
            for item in self.file_streamer.get_chunks():
                yield item
            return

        dist.barrier()

        start_time = timer()
        
        # --- PIPELINE & BATCHING SETUP ---
        # TODO (Noa) check that the available device memory is enough for the buffers
        MAX_CHUNKS_PER_BATCH = 256
        BUFFER_MIN_BYTESIZE = 1024 * 1024 * 1024;
        BUFFER_BYTESIZE = max(BUFFER_MIN_BYTESIZE, self.max_chunk)
        print(f"rank {self.rank} broadcasting buffer size: {BUFFER_BYTESIZE}", flush=True)

        data_buffer = torch.empty(BUFFER_BYTESIZE, dtype=torch.uint8, device=self.device_type)
        received_buffer = torch.empty(BUFFER_BYTESIZE, dtype=torch.uint8, device=self.device_type)

        print(f"rank {self.rank} allocating reusable buffers  humanized: {humanize.naturalsize(2 * BUFFER_BYTESIZE)}", flush=True)
  
        batch_metadata_tensor = torch.zeros(MAX_CHUNKS_PER_BATCH + 1, 4, dtype=torch.int64, device=self.device_type)
        received_metadata_tensor = torch.zeros(MAX_CHUNKS_PER_BATCH + 1, 4, dtype=torch.int64, device=self.device_type)
        
        # --- END OF SETUP ---

        ready_chunks_iterator = self.file_streamer.get_chunks()
        
        def chunk_generator():
            yield from ready_chunks_iterator
            while True:
                yield None

        group_size = self.get_group_size()
        total_chunks = len(self.broadcast_plan)
        chunk_gen = chunk_generator()
        is_iterator_exhausted = False
        leftover_chunk = None
        print(f"rank {self.rank} should read {total_chunks} chunks ")

        try:
            for i in range(len(self.broadcast_plan)):
               
                    # --- Fill staging buffer ---
                    if total_chunks == 0:
                        break   

                    current_data_offset = 0
                    chunk_count_in_batch = 0
                    
                    if self.reading_from_storage and not is_iterator_exhausted:
                        start_time = timer()
                        copy_batch_time = 0
                        while chunk_count_in_batch < MAX_CHUNKS_PER_BATCH:
 
                            # Prioritize the leftover chunk from the previous batch.
                            if leftover_chunk:
                                chunk_item = leftover_chunk
                                leftover_chunk = None
                            else:
                                chunk_item = next(chunk_gen)

                            if chunk_item is None:
                                is_iterator_exhausted = True
                                break

                            ready_request_id, ready_chunk_index, cpu_buffer = chunk_item
                            chunk_size = cpu_buffer.numel()

                            if current_data_offset + chunk_size > BUFFER_BYTESIZE:
                                # This chunk doesn't fit, so we save it for the next batch.
                                leftover_chunk = chunk_item
                                break 

                            # TODO (Noa) for limited memory mode perform synchronous copy
                            
                            start_copy_time = timer()
                            data_buffer[current_data_offset : current_data_offset + chunk_size].copy_(cpu_buffer.squeeze())
                            
                            orig_req_idx, orig_chunk_idx, _ = self.rank_dicts_map[ready_request_id][ready_chunk_index]
                            # TODO (Noa) for only limited memory mode perform synchronous copy                            
                            batch_metadata_tensor[chunk_count_in_batch + 1].copy_(
                                torch.tensor([orig_req_idx, orig_chunk_idx, chunk_size, current_data_offset])
                            )
                            copy_batch_time += end_copy_time - start_copy_time
                            end_copy_time = timer()
                            
                            current_data_offset += chunk_size
                            chunk_count_in_batch += 1

                        if chunk_count_in_batch > 0:
                            end_time = timer()
                            print(f"rank {self.rank} aggregated {chunk_count_in_batch} chunks in {end_time - start_time} seconds")
                            print(f"rank {self.rank} copy time: {copy_batch_time} seconds")
                    batch_metadata_tensor[0, 0] = chunk_count_in_batch

                    total_chunks -= chunk_count_in_batch                    

                    # --- Broadcast ---

                    print(f"rank {self.rank} broadcasting to {group_size} ranks", flush=True)
                    for broadcasting_rank in range(group_size):
                        if broadcasting_rank == self.rank:

                            # broadcast metadata
                            dist.broadcast(batch_metadata_tensor, self.rank)

                            # broadcast data
                            if chunk_count_in_batch > 0:
                                dist.broadcast(data_buffer[:current_data_offset], self.rank)

                            # yield
                                # after the synvhronous broadcast the asynchronous copy has finished and it is safe to yield
                                for j in range(chunk_count_in_batch):
                                    meta = batch_metadata_tensor[j + 1]
                                    offset, size = meta[3].item(), meta[2].item()
                                    yield meta[0].item(), meta[1].item(), data_buffer[offset : offset + size]
                        else:
                            # receive metadata
                            dist.broadcast(received_metadata_tensor, broadcasting_rank)

                            received_chunk_count_in_batch = received_metadata_tensor[0, 0].item()

                            if received_chunk_count_in_batch == 0:
                                continue

                            # receive data
                            last_meta = received_metadata_tensor[received_chunk_count_in_batch]
                            total_data_size = last_meta[3].item() + last_meta[2].item() # offset plus size of last chunk in batch

                            total_chunks -= received_chunk_count_in_batch

                            received_data_buf_view = received_buffer[:total_data_size]

                            dist.broadcast(received_data_buf_view, broadcasting_rank)

                            for j in range(received_chunk_count_in_batch):
                                meta = received_metadata_tensor[j + 1]
                                offset, size = meta[3].item(), meta[2].item()
                                yield meta[0].item(), meta[1].item(), received_data_buf_view[offset : offset + size]

        finally:
            dist.barrier()
            end_time = timer()
            print(f"rank {self.rank} done in {end_time - start_time} seconds")
