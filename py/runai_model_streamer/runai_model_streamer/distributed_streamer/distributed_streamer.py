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
import time

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
        else:
            print(f"rank {self.rank} has no files to stream")
 
    # def get_chunks(self) -> Iterator:
    #     if not self.file_streamer:
    #         raise ValueError("Streamer not initialized")
        
    #     if not self.is_distributed:
    #         for file_path, ready_chunk_index, buffer in self.file_streamer.get_chunks():
    #             yield file_path, ready_chunk_index, buffer
    #         return

    #     # wait for all processes to reach this point
    #     dist.barrier()
        
    #     # allocate metadata buffer for three integers according to the device type
    #     metadata_buffer = torch.zeros(3, dtype=torch.int64, device=self.device_type)

    #     # broadcast and wait for ready tensors
    #     # TODO (Noa) : Handle exceptions by the file streamer, broadcast error code in case of a failure to obtain the chunk, broadcast with timeout in case a process terminates to avoid hanging processes

    #     ready_chunks_iterator = self.file_streamer.get_chunks()

    #     total_wait_time = 0
    #     total_broadcast_time = 0

    #     for i in range(len(self.broadcast_plan)):
    #         if self.broadcast_plan[i] == self.rank:
    #             # wait for the next chunk
        
    #             try:
    #                 ready_request_index, ready_chunk_index, buffer = next(ready_chunks_iterator)
    #             except ValueError as e:    
    #                 return

    #             # translate chunk index to original request index, chunk index and size
    #             original_request_index, original_chunk_index, original_chunk_size = self.rank_dicts_map[ready_request_index][ready_chunk_index]

    #             start_time = timer()
    #             # broadcast chunk identifiers
    #             metadata_buffer[0] = original_request_index
    #             metadata_buffer[1] = original_chunk_index
    #             metadata_buffer[2] = original_chunk_size
    #             dist.broadcast(metadata_buffer, self.rank)
    #             # broadcast data
    #             buffer.to(self.device_type)
    #             dist.broadcast(buffer, self.rank)
    #             end_time = timer()
    #             total_broadcast_time += end_time - start_time
    #             print(f"brodcasted buffer after waiting {end_time - start_time} seconds - rank {self.rank} -  original_request_index {original_request_index} original_chunk_index {original_chunk_index} original_chunk_size {original_chunk_size}", flush=True)
    #             yield original_request_index, original_chunk_index, buffer
    #         else:
    #             # wait for broadcast chunk identifiers
    #             start_time = timer()
    #             metadata_buffer.to(self.device_type)
    #             dist.broadcast(metadata_buffer, self.broadcast_plan[i])
    #             # Use .item() to get scalar values without a full tensor copy
    #             original_request_index = metadata_buffer[0].item()
    #             original_chunk_index = metadata_buffer[1].item()
    #             original_chunk_size = metadata_buffer[2].item()

    #             # allocate buffer for the chunk on the correct device
    #             # TODO (Noa) : allocate a buffer in the size of the largest chunk and reuse it instead of allocating a new one each time
    #             buffer = torch.empty(original_chunk_size, dtype=torch.uint8, device=self.device_type)

    #             # wait for broadcast data
    #             dist.broadcast(buffer, self.broadcast_plan[i])

    #             end_time = timer()
    #             total_wait_time += end_time - start_time
    #             print(f"got buffer after waiting {end_time - start_time} seconds - rank {self.rank} -  original_request_index {original_request_index} original_chunk_index {original_chunk_index} original_chunk_size {original_chunk_size}", flush=True)
    #             yield original_request_index, original_chunk_index, buffer
    #     print(f"******************* rank {self.rank} total wait time: {total_wait_time} seconds ; total broadcast time: {total_broadcast_time} seconds")


    # def get_chunks(self) -> Iterator:
    #     if not self.file_streamer:
    #         raise ValueError("Streamer not initialized")
        
    #     if not self.is_distributed:
    #         # Non-distributed logic remains the same
    #         for item in self.file_streamer.get_chunks():
    #             yield item
    #         return

    #     barrier_time = timer()
    #     dist.barrier()
    #     end_barrier_time = timer()
    #     total_barrier_time = end_barrier_time - barrier_time
    #     print(f"******************* rank {self.rank} total barrier time: {total_barrier_time} seconds")
        
    #     metadata_buffer = torch.zeros(3, dtype=torch.int64, device=self.device_type)

    #     # 1. Allocate a single, reusable buffer for the largest possible chunk.
    #     reusable_buffer = torch.empty(self.max_chunk, dtype=torch.uint8, device=self.device_type)

    #     start_ready_chunks_iterator_time = timer()
    #     ready_chunks_iterator = self.file_streamer.get_chunks()
    #     end_ready_chunks_iterator_time = timer()
    #     total_ready_chunks_iterator_time = end_ready_chunks_iterator_time - start_ready_chunks_iterator_time
    #     print(f"******************* rank {self.rank} total ready chunks iterator time: {total_ready_chunks_iterator_time} seconds")

    #     total_read_time = 0
    #     total_send_time = 0
    #     total_send_metadata_time = 0
    #     total_copy_time = 0
    #     total_receive_time = 0
    #     total_receive_metadata_time = 0
    #     total_time = 0

    #     start_total_time = timer()
    #     for i in range(len(self.broadcast_plan)):
    #         if self.broadcast_plan[i] == self.rank:

    #             # TODO(Noa) 
    #             # use asynchronous broadcasting to fill the reusable buffer as much as possible -> overlap tensor copy and broadcasts
    #             # --- SENDER LOGIC ---
    #             start_time = timer()             
    #             ready_request_index, ready_chunk_index, cpu_buffer = next(ready_chunks_iterator)
    #             end_time = timer()
    #             total_read_time += end_time - start_time
    #             original_request_index, original_chunk_index, original_chunk_size = self.rank_dicts_map[ready_request_index][ready_chunk_index]
    #             # print chunk byte size in human readable format
    #             # print(f"original_chunk_size: {humanize.naturalsize(original_chunk_size, binary=True)}")

    #             # 2. Create a zero-copy view of the correct size
    #             buffer_view = reusable_buffer[:original_chunk_size]
                
    #             # Broadcast metadata
    #             start_time = timer()
    #             metadata_buffer[0] = original_request_index
    #             metadata_buffer[1] = original_chunk_index
    #             metadata_buffer[2] = original_chunk_size
    #             dist.broadcast(metadata_buffer, self.rank)
    #             end_time = timer()
    #             total_send_metadata_time += end_time - start_time

    #             # 3. Copy the streamed data from the CPU buffer into the GPU view
    #             start_time = timer()
    #             buffer_view.copy_(cpu_buffer.squeeze())
    #             end_time = timer()
    #             total_copy_time += end_time - start_time
                
    #             # 4. Broadcast the view
    #             start_time = timer()
    #             dist.broadcast(buffer_view, self.rank)
    #             end_time = timer()
    #             total_send_time += end_time - start_time
                
    #             yield original_request_index, original_chunk_index, buffer_view
    #         else:
    #             # --- RECEIVER LOGIC ---
    #             # Wait for metadata
    #             start_time = timer()
    #             dist.broadcast(metadata_buffer, self.broadcast_plan[i])
                
    #             original_request_index = metadata_buffer[0].item()
    #             original_chunk_index = metadata_buffer[1].item()
    #             original_chunk_size = metadata_buffer[2].item()

    #             end_time = timer()
    #             total_receive_metadata_time += end_time - start_time

    #             # 5. Create a zero-copy view to receive the data
    #             buffer_view = reusable_buffer[:original_chunk_size]

    #             # Wait for broadcast data to fill the view
    #             start_time = timer()
    #             dist.broadcast(buffer_view, self.broadcast_plan[i])
    #             end_time = timer()
    #             total_receive_time += end_time - start_time
                
    #             yield original_request_index, original_chunk_index, buffer_view

    #     end_time = timer()
    #     total_time = end_time - start_total_time
    #     print(f"******************* rank {self.rank} total read time: {total_read_time} seconds ; total send time: {total_send_time} seconds ; total send metadata time: {total_send_metadata_time} seconds ; total copy time: {total_copy_time} seconds ; total receive time: {total_receive_time} seconds ; total receive metadata time: {total_receive_metadata_time} seconds ; total time: {total_time} seconds")

    # def get_chunks(self) -> Iterator:
    #     if not self.file_streamer:
    #         raise ValueError("Streamer not initialized")
        
    #     if not self.is_distributed:
    #         for item in self.file_streamer.get_chunks():
    #             yield item
    #         return

    #     dist.barrier()
        
    #     # --- PIPELINE SETUP ---
    #     # Define how many broadcasts can be "in-flight" at once.
    #     # A small number like 2-4 is usually sufficient.
    #     PIPELINE_DEPTH = 8
        
    #     # 1. Create a pool of reusable buffers for data and metadata.
    #     metadata_buffers = [
    #         torch.zeros(3, dtype=torch.int64, device=self.device_type)
    #         for _ in range(PIPELINE_DEPTH)
    #     ]
    #     data_buffers = [
    #         torch.empty(self.max_chunk, dtype=torch.uint8, device=self.device_type)
    #         for _ in range(PIPELINE_DEPTH)
    #     ]
        
    #     # List to hold the handles of in-flight async operations.
    #     handles = []
    #     # --- END OF SETUP ---

    #     ready_chunks_iterator = self.file_streamer.get_chunks()

    #     for i in range(len(self.broadcast_plan)):
    #         broadcasting_rank = self.broadcast_plan[i]
            
    #         # Use buffers from the pool in a round-robin fashion.
    #         buffer_idx = i % PIPELINE_DEPTH

    #         if broadcasting_rank == self.rank:
    #             # --- SENDER LOGIC ---
                
    #             # 2. Before using a buffer, wait for its previous operation to complete.
    #             #    This ensures we don't overwrite data that's still being sent.
    #             if len(handles) >= PIPELINE_DEPTH:
    #                 handles.pop(0).wait() # Wait for the oldest handle

    #             try:
    #                 ready_request_id, ready_chunk_index, cpu_buffer = next(ready_chunks_iterator)
    #             except StopIteration:
    #                 break

    #             orig_req_idx, orig_chunk_idx, orig_chunk_size = self.rank_dicts_map[ready_request_id][ready_chunk_index]

    #             # Prepare metadata and data in the selected buffers
    #             metadata_buf = metadata_buffers[buffer_idx]
    #             metadata_buf[0] = orig_req_idx
    #             metadata_buf[1] = orig_chunk_idx
    #             metadata_buf[2] = orig_chunk_size
                
    #             data_buf_view = data_buffers[buffer_idx][:orig_chunk_size]
    #             data_buf_view.copy_(cpu_buffer.squeeze(), non_blocking=True)

    #             # 3. Launch the asynchronous broadcasts and store the handle.
    #             #    We only need to track the handle for the data, as it's the larger transfer.
    #             metadata_handle = dist.broadcast(metadata_buf, self.rank, async_op=True)
    #             #handles.append(metadata_handle)
    #             data_handle = dist.broadcast(data_buf_view, self.rank, async_op=True)
    #             handles.append(data_handle)

    #             # The sender yields immediately after launching.
    #             yield orig_req_idx, orig_chunk_idx, data_buf_view

    #         else:
    #             # --- RECEIVER LOGIC ---
    #             # Receivers use synchronous calls, which will correctly pair with the sender's async calls.
    #             metadata_buf = metadata_buffers[buffer_idx]
    #             dist.broadcast(metadata_buf, broadcasting_rank)
                
    #             received_request_index = metadata_buf[0].item()
    #             received_chunk_index = metadata_buf[1].item()
    #             received_chunk_size = metadata_buf[2].item()
                
    #             data_buf_view = data_buffers[buffer_idx][:received_chunk_size]
    #             dist.broadcast(data_buf_view, broadcasting_rank)
                
    #             yield received_request_index, received_chunk_index, data_buf_view

    #     # After the loop, the sender must wait for any remaining in-flight operations.
    #     if self.rank in self.broadcast_plan:
    #         for handle in handles:
    #             handle.wait()


    # 7 seconds and best memory usage of gpu

    # def get_chunks(self) -> Iterator:
    #     if not self.file_streamer:
    #         raise ValueError("Streamer not initialized")
        
    #     if not self.is_distributed:
    #         for item in self.file_streamer.get_chunks():
    #             yield item
    #         return

    #     dist.barrier()
        
    #     # --- PIPELINE SETUP ---
    #     # Define how many broadcasts can be "in-flight" at once.
    #     PIPELINE_DEPTH = 50
        
    #     # 1. Create a pool for metadata buffers and a SINGLE reusable data buffer.
    #     metadata_buffers = [
    #         torch.zeros(4, dtype=torch.int64, device=self.device_type)
    #         for _ in range(PIPELINE_DEPTH)
    #     ]
    #     data_buffer_size = self.max_chunk
    #     data_buffer = torch.empty(data_buffer_size, dtype=torch.uint8, device=self.device_type)
        
    #     # List to hold the handles of in-flight async operations for the sender.
    #     handles = []
    #     # --- END OF SETUP ---

    #     ready_chunks_iterator = self.file_streamer.get_chunks()

    #     # Senders and receivers must independently track the state of the buffer.
    #     current_buffer_offset = 0

    #     for i in range(len(self.broadcast_plan)):
    #         broadcasting_rank = self.broadcast_plan[i]
            
    #         if broadcasting_rank == self.rank:
    #             # --- SENDER LOGIC ---
    #             try:
    #                 ready_request_id, ready_chunk_index, cpu_buffer = next(ready_chunks_iterator)
    #             except StopIteration:
    #                 break

    #             orig_req_idx, orig_chunk_idx, orig_chunk_size = self.rank_dicts_map[ready_request_id][ready_chunk_index]

    #             # 2. Check if we need to wait and reset the buffer.
    #             # This happens if the pipeline is full OR the next chunk won't fit.
    #             if len(handles) >= PIPELINE_DEPTH or (current_buffer_offset + orig_chunk_size > data_buffer_size):
    #                 print(f"waiting for {len(handles)} in-flight broadcasts to complete")
    #                 for handle in handles:
    #                     handle.wait()
    #                 handles.clear()
    #                 current_buffer_offset = 0

    #             # 3. Prepare metadata and data in the selected buffers.
    #             # The metadata buffer is chosen based on the number of current in-flight operations.
    #             metadata_buf_idx = len(handles)
    #             metadata_buf = metadata_buffers[metadata_buf_idx]
    #             metadata_buf[0] = orig_req_idx
    #             metadata_buf[1] = orig_chunk_idx
    #             metadata_buf[2] = orig_chunk_size
    #             metadata_buf[3] = current_buffer_offset # Send the offset

    #             data_buf_view = data_buffer[current_buffer_offset : current_buffer_offset + orig_chunk_size]
    #             # The copy to the GPU must be non-blocking to allow overlap.
    #             # This assumes the source cpu_buffer is in pinned memory.
    #             data_buf_view.copy_(cpu_buffer.squeeze(), non_blocking=True)

    #             # 4. Launch the asynchronous broadcasts and store the handle.
    #             dist.broadcast(metadata_buf, self.rank, async_op=True)
    #             data_handle = dist.broadcast(data_buf_view, self.rank, async_op=True)
    #             handles.append(data_handle)

    #             # 5. Update the buffer offset for the next chunk.
    #             current_buffer_offset += orig_chunk_size

    #             # The sender yields its local view immediately.
    #             yield orig_req_idx, orig_chunk_idx, data_buf_view

    #         else:
    #             # --- RECEIVER LOGIC ---
    #             # The receiver must use a local counter to select the correct metadata buffer from its pool.
    #             # This is implicitly handled by the global step `i` and the broadcast plan.
    #             # We need to determine which of the sender's "in-flight" sends this is.
    #             # A simpler, robust way is to use a single metadata buffer and make its broadcast synchronous.
                
    #             # For simplicity and correctness, we make the metadata broadcast synchronous for the receiver.
    #             # The performance gain comes from overlapping the large data broadcasts.
    #             temp_metadata_buf = torch.zeros(4, dtype=torch.int64, device=self.device_type)
    #             dist.broadcast(temp_metadata_buf, broadcasting_rank)
                
    #             received_request_index = temp_metadata_buf[0].item()
    #             received_chunk_index = temp_metadata_buf[1].item()
    #             received_chunk_size = temp_metadata_buf[2].item()
    #             received_offset = temp_metadata_buf[3].item()
                
    #             # The receiver uses the received offset to create the correct view.
    #             data_buf_view = data_buffer[received_offset : received_offset + received_chunk_size]
    #             dist.broadcast(data_buf_view, broadcasting_rank)
                
    #             yield received_request_index, received_chunk_index, data_buf_view

    #     # After the loop, the sender must wait for any remaining in-flight operations.
    #     for handle in handles:
    #         handle.wait()


    # 5.5 but hanging with deadlock

    # def get_chunks(self) -> Iterator:
    #     if not self.file_streamer:
    #         raise ValueError("Streamer not initialized")
        
    #     if not self.is_distributed:
    #         for item in self.file_streamer.get_chunks():
    #             yield item
    #         return

    #     dist.barrier()
        
    #     # --- PIPELINE & BATCHING SETUP ---
    #     PIPELINE_DEPTH = 2 # How many full buffer broadcasts can be in-flight.
    #     # Max number of chunks that can be packed into a single broadcast.
    #     MAX_CHUNKS_PER_BATCH = 50 
        
    #     # 1. Create a pool of reusable buffers.
    #     data_buffers = [
    #         torch.empty(self.max_chunk, dtype=torch.uint8, device=self.device_type)
    #         for _ in range(PIPELINE_DEPTH)
    #     ]
    #     # Metadata buffer: [chunk_count, req_idx, chunk_idx, size, offset]
    #     # We use a 2D tensor to store metadata for each chunk in a batch.
    #     metadata_buffers = [
    #         torch.zeros(MAX_CHUNKS_PER_BATCH + 1, 4, dtype=torch.int64, device=self.device_type)
    #         for _ in range(PIPELINE_DEPTH)
    #     ]
        
    #     handles = []
    #     # --- END OF SETUP ---

    #     ready_chunks_iterator = self.file_streamer.get_chunks()
        
    #     def chunk_generator():
    #         yield from ready_chunks_iterator
    #         while True:
    #             yield None

    #     chunk_gen = chunk_generator()
    #     is_iterator_exhausted = False
    #     send_count = 0

    #     for i in range(len(self.broadcast_plan)):
    #         broadcasting_rank = self.broadcast_plan[i]
    #         buffer_idx = i % PIPELINE_DEPTH

    #         if broadcasting_rank == self.rank:
    #             # --- SENDER LOGIC ---
    #             if is_iterator_exhausted:
    #                 continue

    #             if send_count >= PIPELINE_DEPTH:
    #                 handles.pop(0).wait()

    #             batch_metadata_tensor = metadata_buffers[buffer_idx]
    #             data_buffer = data_buffers[buffer_idx]
                
    #             current_data_offset = 0
    #             chunk_count_in_batch = 0
                
    #             # Pack as many chunks as possible into the current buffer.
    #             while chunk_count_in_batch < MAX_CHUNKS_PER_BATCH:
    #                 chunk_item = next(chunk_gen)
    #                 if chunk_item is None:
    #                     is_iterator_exhausted = True
    #                     break

    #                 ready_request_id, ready_chunk_index, cpu_buffer = chunk_item
    #                 chunk_size = cpu_buffer.numel()

    #                 if current_data_offset + chunk_size > self.max_chunk:
    #                     break 

    #                 # Copy data into the buffer. Assumes cpu_buffer is pinned for async copy.
    #                 data_buffer[current_data_offset : current_data_offset + chunk_size].copy_(cpu_buffer.squeeze(), non_blocking=True)
                    
    #                 orig_req_idx, orig_chunk_idx, _ = self.rank_dicts_map[ready_request_id][ready_chunk_index]
                    
    #                 # Record metadata directly into the tensor.
    #                 batch_metadata_tensor[chunk_count_in_batch + 1].copy_(
    #                     torch.tensor([orig_req_idx, orig_chunk_idx, chunk_size, current_data_offset])
    #                 )
                    
    #                 current_data_offset += chunk_size
    #                 chunk_count_in_batch += 1

    #             if chunk_count_in_batch == 0:
    #                 continue

    #             # Set the number of chunks in the first row of the metadata.
    #             batch_metadata_tensor[0, 0] = chunk_count_in_batch

    #             # Asynchronously broadcast metadata and data.
    #             dist.broadcast(batch_metadata_tensor, self.rank, async_op=True)
    #             data_handle = dist.broadcast(data_buffer[:current_data_offset], self.rank, async_op=True)
    #             handles.append(data_handle)
    #             send_count += 1

    #             # Yield the chunks from the local GPU buffer.
    #             for j in range(chunk_count_in_batch):
    #                 meta = batch_metadata_tensor[j + 1]
    #                 offset, size = meta[3].item(), meta[2].item()
    #                 yield meta[0].item(), meta[1].item(), data_buffer[offset : offset + size]

    #         else:
    #             # --- RECEIVER LOGIC ---
    #             metadata_buf = metadata_buffers[buffer_idx]
    #             dist.broadcast(metadata_buf, broadcasting_rank)
                
    #             chunk_count_in_batch = metadata_buf[0, 0].item()
    #             if chunk_count_in_batch == 0:
    #                 continue

    #             # Calculate total data size from the last metadata entry.
    #             last_meta = metadata_buf[chunk_count_in_batch]
    #             total_data_size = last_meta[3].item() + last_meta[2].item()
                
    #             data_buf_view = data_buffers[buffer_idx][:total_data_size]
    #             dist.broadcast(data_buf_view, broadcasting_rank)
                
    #             # Yield views for each chunk in the batch.
    #             for j in range(chunk_count_in_batch):
    #                 meta = metadata_buf[j + 1]
    #                 offset, size = meta[3].item(), meta[2].item()
    #                 yield meta[0].item(), meta[1].item(), data_buf_view[offset : offset + size]

    #     # Wait for any final in-flight operations.
    #     for handle in handles:
    #         handle.wait()

 
    def get_chunks(self) -> Iterator:
        if not self.file_streamer:
            raise ValueError("Streamer not initialized")
        
        if not self.is_distributed:
            for item in self.file_streamer.get_chunks():
                yield item
            return

        dist.barrier()

        start_time = timer()
        
        # --- PIPELINE & BATCHING SETUP ---
        PIPELINE_DEPTH = 4
        MAX_CHUNKS_PER_BATCH = 256
        
        data_buffers = [
            torch.empty(self.max_chunk, dtype=torch.uint8, device=self.device_type)
            for _ in range(PIPELINE_DEPTH)
        ]
        metadata_buffers = [
            torch.zeros(MAX_CHUNKS_PER_BATCH + 1, 4, dtype=torch.int64, device=self.device_type)
            for _ in range(PIPELINE_DEPTH)
        ]
        
        handles = []
        # --- END OF SETUP ---

        ready_chunks_iterator = self.file_streamer.get_chunks()
        
        def chunk_generator():
            yield from ready_chunks_iterator
            while True:
                yield None

        chunk_gen = chunk_generator()
        is_iterator_exhausted = False
        leftover_chunk = None
        total_chunks = len(self.broadcast_plan)
        print(f"rank {self.rank} should read {total_chunks} chunks")

        try:
            for i in range(len(self.broadcast_plan)):
                broadcasting_rank = self.broadcast_plan[i]
                buffer_idx = i % PIPELINE_DEPTH

                if broadcasting_rank == self.rank:
                    # --- SENDER LOGIC ---

                    if total_chunks == 0:
                        print(f"broadcasting rank {self.rank} finished sending - total_chunks == 0")
                        break   

                    if len(handles) >= PIPELINE_DEPTH:
                        handles.pop(0).wait()

                    batch_metadata_tensor = metadata_buffers[buffer_idx]
                    data_buffer = data_buffers[buffer_idx]
                    
                    current_data_offset = 0
                    chunk_count_in_batch = 0
                    
                    if not is_iterator_exhausted:
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

                            if current_data_offset + chunk_size > self.max_chunk:
                                # This chunk doesn't fit, so we save it for the next batch.
                                leftover_chunk = chunk_item
                                break 

                            data_buffer[current_data_offset : current_data_offset + chunk_size].copy_(cpu_buffer.squeeze(), non_blocking=True)
                            
                            orig_req_idx, orig_chunk_idx, _ = self.rank_dicts_map[ready_request_id][ready_chunk_index]
                            
                            batch_metadata_tensor[chunk_count_in_batch + 1].copy_(
                                torch.tensor([orig_req_idx, orig_chunk_idx, chunk_size, current_data_offset])
                            )
                            
                            current_data_offset += chunk_size
                            chunk_count_in_batch += 1

                    batch_metadata_tensor[0, 0] = chunk_count_in_batch
                    total_chunks -= chunk_count_in_batch
               
                    # The sender launches two async broadcasts.
                    print(f"rank {self.rank} broadcasting metadata from rank {broadcasting_rank} chunk_count_in_batch: {chunk_count_in_batch} total_chunks: {total_chunks}")
                    dist.broadcast(batch_metadata_tensor, self.rank, async_op=True)

                    if chunk_count_in_batch > 0:
                        data_handle = dist.broadcast(data_buffer[:current_data_offset], self.rank, async_op=True)
                        handles.append(data_handle)

                        # The sender can yield its local data immediately.
                        for j in range(chunk_count_in_batch):
                            meta = batch_metadata_tensor[j + 1]
                            offset, size = meta[3].item(), meta[2].item()
                            yield meta[0].item(), meta[1].item(), data_buffer[offset : offset + size]

                else:
                    # --- RECEIVER LOGIC ---
                    print(f"rank {self.rank} waiting for metadata from rank {broadcasting_rank} total_chunks: {total_chunks}")
                    if total_chunks == 0:
                        print(f"rank {self.rank} finished receiving all chunks - total_chunks == 0")
                        break

                    metadata_buf = metadata_buffers[buffer_idx]
                    dist.broadcast(metadata_buf, broadcasting_rank)
                    
                    chunk_count_in_batch = metadata_buf[0, 0].item()

                    print(f"rank {self.rank} received metadata from rank {broadcasting_rank} chunk_count_in_batch: {chunk_count_in_batch} total_chunks: {total_chunks}")
                    
                    if chunk_count_in_batch == 0:
                        print(f"rank {self.rank} received metadata from rank {broadcasting_rank} chunk_count_in_batch == 0")
                        continue

                    print(f"rank {self.rank} received metadata from rank {broadcasting_rank} chunk_count_in_batch: {chunk_count_in_batch} total_chunks: {total_chunks}")

                    last_meta = metadata_buf[chunk_count_in_batch]
                    total_data_size = last_meta[3].item() + last_meta[2].item() # offset plus size of last chunk in batch

                    data_buf_view = data_buffers[buffer_idx][:total_data_size]
                    dist.broadcast(data_buf_view, broadcasting_rank)
                    
                    for j in range(chunk_count_in_batch):
                        meta = metadata_buf[j + 1]
                        offset, size = meta[3].item(), meta[2].item()
                        yield meta[0].item(), meta[1].item(), data_buf_view[offset : offset + size]

                    total_chunks -= chunk_count_in_batch
        finally:
            # Wait for any final in-flight operations from the sender.
            print(f"rank {self.rank} waiting for {len(handles)} in-flight operations to complete")
            for handle in handles:
                handle.wait()
            dist.barrier()
            end_time = timer()
            print(f"rank {self.rank} done waiting for {len(handles)} in-flight operations to complete in {end_time - start_time} seconds")
