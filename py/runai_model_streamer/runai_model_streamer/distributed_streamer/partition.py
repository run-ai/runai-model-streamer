from __future__ import annotations
import os
import dataclasses
from collections import defaultdict
from typing import List, Dict
import humanize
from runai_model_streamer.file_streamer import FileChunks

import logging

logger = logging.getLogger(__name__)

@dataclasses.dataclass(frozen=True)
class _WorkUnit:
    """
    An internal, flattened representation of a single, indivisible chunk of work.
    This simplifies the partitioning logic by breaking down the input into its
    smallest components and tracking their original positions.
    """
    path: str
    offset: int
    size: int
    original_request_index: int # the FileChunks original id
    original_chunk_index: int   # the chunk index in the FileChunks

def partition_by_chunks(
    file_stream_requests: List[FileChunks], n: int
) -> List[List[Tuple[FileChunks, Dict[int, Tuple[int, int, int]]]]]:
    """
    Partitions a list of file read requests into n balanced parts.

    For each returned FileChunks object, it also provides a map from the index
    of a chunk in its new list to a tuple representing its original position
    (original_request_index, original_chunk_index, chunk_size).

    A greedy algorithm is used to assign each chunk to a partition.
    It iterates through the global sorted list of chunks (starting with the largest chunk)
    and assigns each chunk to the partition that is currently the "emptiest"
    (has the smallest total size of assigned work so far).

    Args:
        file_stream_requests: A list of FileChunks objects representing the
                              total work to be done.
        n: The number of partitions to divide the work into.

    Returns:
        A list of n partitions. Each partition is a list of tuples, where each
        tuple contains a new FileChunks object and its corresponding source map.
        The map's key is the new chunk index, and the value is a tuple
        (original request index, original chunk index, chunk size).

    Raises:
        ValueError: If n is not a positive integer.
    """
    if n <= 0:
        raise ValueError("Number of partitions (n) must be a positive integer.")
    
    if not file_stream_requests:
        return [[] for _ in range(n)]

    # 1. Flatten the input `FileChunks` into a single list of `_WorkUnit`s.
    all_units: List[_WorkUnit] = []
    for req_idx, request in enumerate(file_stream_requests):
        current_offset = request.offset
        for chunk_idx, chunk_size in enumerate(request.chunks):
            if chunk_size > 0:
                all_units.append(_WorkUnit(
                    path=request.path,
                    offset=current_offset,
                    size=chunk_size,
                    original_request_index=request.id,
                    original_chunk_index=chunk_idx
                ))
            current_offset += chunk_size

    # 2. Sort the atomic work units from largest to smallest.
    all_units.sort(key=lambda u: u.size, reverse=True)

    # 3. Distribute the sorted work units into n partitions.
    partitions_of_units: List[List[_WorkUnit]] = [[] for _ in range(n)]
    partition_sizes: List[int] = [0] * n

    for unit in all_units:
        min_size_idx = partition_sizes.index(min(partition_sizes))
        partitions_of_units[min_size_idx].append(unit)
        partition_sizes[min_size_idx] += unit.size

    # 4. Reconstruct the final `FileChunks` objects and their source maps.
    result_partitions: List[List[Tuple[FileChunks, Dict[int, Tuple[int, int, int]]]]] = []
    id_generator = 0
    for partition_of_units in partitions_of_units:
        new_partition: List[Tuple[FileChunks, Dict[int, Tuple[int, int, int]]]] = []
        units_by_path: Dict[str, List[_WorkUnit]] = defaultdict(list)
        for unit in partition_of_units:
            units_by_path[unit.path].append(unit)
        
        for path, units in units_by_path.items():
            units.sort(key=lambda u: u.offset)
            if not units:
                continue

            current_fc = FileChunks(id=id_generator, path=path, offset=units[0].offset, chunks=[units[0].size])
            current_map = {0: (units[0].original_request_index, units[0].original_chunk_index, units[0].size)}
            id_generator += 1
            
            for i in range(1, len(units)):
                next_unit = units[i]
                if current_fc.offset + sum(current_fc.chunks) == next_unit.offset:
                    new_chunk_index = len(current_fc.chunks)
                    current_fc.chunks.append(next_unit.size)
                    current_map[new_chunk_index] = (next_unit.original_request_index, next_unit.original_chunk_index, next_unit.size)
                else:
                    new_partition.append((current_fc, current_map))
                    current_fc = FileChunks(id=id_generator, path=path, offset=next_unit.offset, chunks=[next_unit.size])
                    current_map = {0: (next_unit.original_request_index, next_unit.original_chunk_index, next_unit.size)}
                    id_generator += 1
            
            new_partition.append((current_fc, current_map))
        
        result_partitions.append(new_partition)

    return result_partitions

def partition_by_files(
    file_stream_requests: List[FileChunks], n: int
) -> List[List[Tuple[FileChunks, Dict[int, Tuple[int, int, int]]]]]:
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
        A list of n partitions. Each partition is a list of tuples, where each
        tuple contains a FileChunks object and its corresponding source map.
    """
    if n <= 0:
        raise ValueError("Number of partitions (n) must be a positive integer.")
    
    if not file_stream_requests:
        return [[] for _ in range(n)]

    # 1. Sort the FileChunks objects from largest to smallest total size, keeping track of original index.
    requests_with_indices = list(enumerate(file_stream_requests))
    sorted_requests = sorted(
        requests_with_indices,
        key=lambda item: sum(item[1].chunks),
        reverse=True
    )

    # 2. Distribute the sorted FileChunks objects into n partitions.
    partitions: List[List[Tuple[FileChunks, Dict[int, Tuple[int, int, int]]]]] = [[] for _ in range(n)]
    partition_sizes: List[int] = [0] * n

    for original_request_index, request in sorted_requests:
        min_size_idx = partition_sizes.index(min(partition_sizes))
        
        # Create the source map. Since we aren't changing the chunk order within
        # the FileChunks object, the mapping is direct.
        source_map = {
            chunk_idx: (original_request_index, chunk_idx, request.chunks[chunk_idx])
            for chunk_idx in range(len(request.chunks))
        }
        
        partitions[min_size_idx].append((request, source_map))
        partition_sizes[min_size_idx] += sum(request.chunks)

    return partitions

# Dict[int, Tuple[int, int, int] maps the chunk index in the corresponding
# FileChunks object to the original request index, chunk index, and chunk size.

def get_partition_policy() -> str:
    partition_policy = os.getenv("RUNAI_STREAMER_PARTITION_POLICY")
    if partition_policy is not None:
        return partition_policy
    else:
        return "chunks"

def partition(file_stream_requests: List[FileChunks], n: int) -> List[List[Tuple[FileChunks, Dict[int, Tuple[int, int, int]]]]]:
    partition_policy = get_partition_policy()
    if partition_policy == "files":
        return partition_by_files(file_stream_requests, n)
    elif partition_policy == "chunks":
        return partition_by_chunks(file_stream_requests, n)
    else:
        raise ValueError(f"Invalid partition policy: {partition_policy}")

def get_total_number_of_chunks(partitions: List[List[Tuple[FileChunks, dict]]]) -> int:
    if partitions is None or len(partitions) == 0:
        return 0
    return sum(sum(len(fc.chunks) for fc, _ in p) for p in partitions)

def get_total_size_of_partition(partition: List[Tuple[FileChunks, dict]]) -> int:
    if partition is None or len(partition) == 0:
        return 0
    return sum(sum(fc.chunks) for fc, _ in partition)

def log_partition_info(partitions: List[List[Tuple[FileChunks, dict]]]):
    log_string = "[RunAI Streamer][Distributed] Partitions sizes:"
    for i in range(len(partitions)):
        size = get_total_size_of_partition(partitions[i])
        log_string += f" {i}: {humanize.naturalsize(size, binary=True)} ; "
    logger.debug(log_string)

