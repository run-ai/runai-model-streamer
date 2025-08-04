from __future__ import annotations
import dataclasses
from collections import defaultdict
from typing import List, Dict

from runai_model_streamer.file_streamer import FileChunks

@dataclasses.dataclass(frozen=True)
class _WorkUnit:
    """
    An internal, flattened representation of a single, indivisible chunk of work.
    This simplifies the partitioning logic by breaking down the input into its
    smallest components.
    """
    path: str
    offset: int
    size: int

def partition_by_chunks(file_stream_requests: List[FileChunks], n: int) -> List[List[FileChunks]]:
    """
    Partitions a list of file read requests into n balanced parts.

    The function prioritizes creating partitions of nearly equal total byte size.
    A secondary goal is to maintain continuous read operations where possible.
    The partitioning is deterministic: the same input will always produce the
    same output.

    Args:
        file_stream_requests: A list of FileChunks objects representing the
                              total work to be done.
        n: The number of partitions to divide the work into.

    Returns:
        A list containing n lists of FileChunks. Each inner list is a partition
        of the total workload.

    Raises:
        ValueError: If n is not a positive integer.
    """
    if n <= 0:
        raise ValueError("Number of partitions (n) must be a positive integer.")
    
    if not file_stream_requests:
        return [[] for _ in range(n)]

    # 1. Flatten the input `FileChunks` into a single list of `_WorkUnit`s.
    # Each WorkUnit is a single, atomic chunk with its absolute file offset.
    all_units: List[_WorkUnit] = []
    for request in file_stream_requests:
        current_offset = request.offset
        for chunk_size in request.chunks:
            if chunk_size > 0:
                all_units.append(_WorkUnit(
                    path=request.path,
                    offset=current_offset,
                    size=chunk_size
                ))
            current_offset += chunk_size

    # 2. Sort the atomic work units. This is the key to the partitioning
    # strategy. By sorting from the largest to the smallest chunk, we can use a
    # greedy algorithm that closely approximates the optimal solution for the
    # bin packing problem, ensuring the most balanced partitions possible.
    all_units.sort(key=lambda u: u.size, reverse=True)

    # 3. Distribute the sorted work units into n partitions.
    # We always add the next work unit to the partition that currently has the
    # smallest total size.
    partitions: List[List[_WorkUnit]] = [[] for _ in range(n)]
    partition_sizes: List[int] = [0] * n

    for unit in all_units:
        # Find the partition with the minimum current workload.
        min_size_idx = partition_sizes.index(min(partition_sizes))
        
        # Assign the work unit and update the partition's size.
        partitions[min_size_idx].append(unit)
        partition_sizes[min_size_idx] += unit.size

    # 4. Reconstruct the final `FileChunks` objects from the partitioned work units.
    # Within each partition, we group units by file path and merge consecutive
    # chunks back together to respect the `FileChunks` format.
    result_partitions: List[List[FileChunks]] = []
    for partition in partitions:
        new_file_chunks_list: List[FileChunks] = []
        
        # Group work units by their file path within the partition.
        units_by_path: Dict[str, List[_WorkUnit]] = defaultdict(list)
        for unit in partition:
            units_by_path[unit.path].append(unit)
        
        # For each file, reconstruct the continuous FileChunks.
        for path, units in units_by_path.items():
            # Sort by offset to find continuous blocks.
            units.sort(key=lambda u: u.offset)

            if not units:
                continue

            # Iterate through the sorted units, merging where possible.
            current_fc = FileChunks(path=path, offset=units[0].offset, chunks=[units[0].size])
            
            for i in range(1, len(units)):
                next_unit = units[i]
                # Check if the next unit is contiguous with the current FileChunks block.
                if current_fc.offset + sum(current_fc.chunks) == next_unit.offset:
                    # If so, append its size to the current block.
                    current_fc.chunks.append(next_unit.size)
                else:
                    # If not, the current block is finished. Add it to our list.
                    new_file_chunks_list.append(current_fc)
                    # Start a new block with the non-contiguous unit.
                    current_fc = FileChunks(path=path, offset=next_unit.offset, chunks=[next_unit.size])
            
            # Add the last processed FileChunks block to the list.
            new_file_chunks_list.append(current_fc)
            
        result_partitions.append(new_file_chunks_list)

    return result_partitions

def partition_by_files(file_stream_requests: List[FileChunks], n: int) -> List[List[FileChunks]]:
    """
    Partitions a list of file read requests into n parts by distributing
    whole FileChunks objects.

    This method preserves the continuity of chunks within a FileChunks object
    but may result in less balanced partitions compared to stream_files.
    The partitioning is deterministic.

    Args:
        file_stream_requests: A list of FileChunks objects representing the
                              total work to be done.
        n: The number of partitions to divide the work into.

    Returns:
        A list containing n lists of FileChunks. Each inner list is a partition
        of the total workload.

    Raises:
        ValueError: If n is not a positive integer.
    """
    if n <= 0:
        raise ValueError("Number of partitions (n) must be a positive integer.")
    
    if not file_stream_requests:
        return [[] for _ in range(n)]

    # 1. Sort the FileChunks objects from largest to smallest total size.
    # We create a copy to avoid modifying the original list.
    sorted_requests = sorted(file_stream_requests, key=lambda fc: fc.total_size(), reverse=True)

    # 2. Distribute the sorted FileChunks objects into n partitions.
    partitions: List[List[FileChunks]] = [[] for _ in range(n)]
    partition_sizes: List[int] = [0] * n

    for request in sorted_requests:
        # Find the partition with the minimum current workload.
        min_size_idx = partition_sizes.index(min(partition_sizes))
        
        # Assign the FileChunks object and update the partition's size.
        partitions[min_size_idx].append(request)
        partition_sizes[min_size_idx] += request.total_size()

    return partitions

def partition(file_stream_requests: List[FileChunks], n: int) -> List[List[FileChunks]]:
    if len(file_stream_requests) >= n:
        return partition_by_files(file_stream_requests, n)
    else:
        return partition_by_chunks(file_stream_requests, n)

