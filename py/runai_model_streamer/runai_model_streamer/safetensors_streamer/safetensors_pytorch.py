from __future__ import annotations
import torch
import struct
import json
from typing import List, Tuple, Optional
from runai_model_streamer.distributed_streamer.distributed_streamer import (DistributedStreamer, FileChunks)

SAFETENSORS_DATA_OFFSETS_KEY = "data_offsets"
SAFETENSORS_NAME_KEY = "name"
SAFETENSORS_SHAPE_KEY = "shape"
SAFETENSORS_DTYPE_KEY = "dtype"
SAFETENSORS_HEADER_BUFFER_SIZE = 8

# Define a safety limit (100MB) for the header
MAX_HEADER_SIZE = 100 * 1024 * 1024 

LITTLE_ENDIAN_LONG_LONG_STRUCT_FORMAT = "<Q"

safetenors_to_torch_dtype = {
    "F64": torch.float64,
    "F32": torch.float32,
    "F16": torch.float16,
    "BF16": torch.bfloat16,
    "I64": torch.int64,
    "I32": torch.int32,
    "I16": torch.int16,
    "I8": torch.int8,
    "U8": torch.uint8,
    "BOOL": torch.bool,
    "F8_E5M2": torch.float8_e5m2,
    "F8_E4M3": torch.float8_e4m3fn,
}

class SafetensorsMetadata:
    def __init__(self, blob: any, offset: int) -> None:
        self.offset = offset
        self.tensors_metadata = []
        self.read_sizes = []

        for name, safetensor_metadata in blob.items():
            if name != "__metadata__":
                # Ensure required keys exist before processing
                if SAFETENSORS_DATA_OFFSETS_KEY not in safetensor_metadata:
                    raise ValueError(f"Corrupted Header: Tensor '{name}' missing {SAFETENSORS_DATA_OFFSETS_KEY}")
                
                tensor_metadata = SafetensorMetadata(name, safetensor_metadata)
                self.tensors_metadata.append(tensor_metadata)

        self.tensors_metadata.sort(key=lambda x: x.offsets.start)

        for i in range(len(self.tensors_metadata)):
            current_tensor = self.tensors_metadata[i]

            # Logical check for individual tensor offsets
            if current_tensor.offsets.start > current_tensor.offsets.end:
                raise ValueError(f"Corrupted Offset: Tensor '{current_tensor.name}' start ({current_tensor.offsets.start}) > end ({current_tensor.offsets.end})")
            
            required_size = current_tensor.get_bytesize()

            if i < len(self.tensors_metadata) - 1:
                current_start = self.tensors_metadata[i].offsets.start
                next_start = self.tensors_metadata[i + 1].offsets.start
                
                # Check for overlapping tensors
                # If tensors overlap, next_start - current_start < size, leading to data corruption.
                if current_start + required_size > next_start:
                    raise ValueError(f"Corrupted File: Tensor '{current_tensor.name}' overlaps with next tensor. (Ends at {current_start + required_size}, next starts at {next_start})")

                # Check for holes/gaps between tensors
                if current_start + required_size < next_start:
                     raise ValueError(f"Corrupted File: Gaps between tensors are not allowed. Tensor '{current_tensor.name}' ends at {current_start + required_size}, but next tensor starts at {next_start}.")

            self.read_sizes.append(required_size)

    @staticmethod
    def from_files(fs: DistributedStreamer, filenames: List[str], s3_credentials: Optional[S3Credentials]) -> List[SafetensorsMetadata]:
        # 1. Read the first 8 bytes (The Header Size)
        fs.stream_files([FileChunks(i, filenames[i], 0, [SAFETENSORS_HEADER_BUFFER_SIZE]) for i in range(len(filenames))], s3_credentials, "cpu", False)
        
        header_sizes = {}
        
        # Wrap the first loop to catch truncation during size read
        try:
            for file_index, ready_chunk_index, buffer in fs.get_chunks():
                if buffer.nelement() < 8:
                    raise ValueError(f"File {filenames[file_index]} is truncated or empty.")

                size = struct.unpack(
                    LITTLE_ENDIAN_LONG_LONG_STRUCT_FORMAT, buffer.numpy()
                )[0]

                if size > MAX_HEADER_SIZE:
                     raise ValueError(f"Corrupted File: Header size {size} in {filenames[file_index]} exceeds limit of {MAX_HEADER_SIZE} bytes.")
                
                header_sizes[file_index] = size
        except Exception as e:
            # Catch the generic libstreamer error and re-raise as ValueError for consistency
            if isinstance(e, ValueError): raise e
            raise ValueError(f"Streamer failed to read header size (likely truncated file): {str(e)}")

        # 2. Read the JSON Header Body
        metadatas = {}
        fs.stream_files([FileChunks(i, filenames[i], SAFETENSORS_HEADER_BUFFER_SIZE, [header_size]) for i, header_size in header_sizes.items()], s3_credentials, "cpu", False)
        
        # Wrap the second loop to catch truncation during JSON read
        try:
            for file_index, ready_chunk_index, buffer in fs.get_chunks():
                try:
                    json_bytes = bytearray(buffer.numpy())
                    json_str = json_bytes.decode('utf-8')
                    metadatas[file_index] = json.loads(json_str)
                except UnicodeDecodeError:
                    raise ValueError(f"Corrupted File: Header in {filenames[file_index]} is not valid UTF-8.")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Corrupted File: Header in {filenames[file_index]} is not valid JSON. Error: {e}")
        except Exception as e:
            if isinstance(e, ValueError): raise e
            raise ValueError(f"Streamer failed to read header body (likely truncated file): {str(e)}")

        return [SafetensorsMetadata(
            metadatas[i], header_sizes[i] + SAFETENSORS_HEADER_BUFFER_SIZE
        ) for i in range(len(filenames))] 

class SafetensorMetadata:
    def __init__(self, name: str, safetensorMetadata: any) -> None:
        self.name = name
        self.shape = safetensorMetadata[SAFETENSORS_SHAPE_KEY]
        self.dtype = safetensorMetadata[SAFETENSORS_DTYPE_KEY]
        self.offsets = Offsets(safetensorMetadata[SAFETENSORS_DATA_OFFSETS_KEY])
        
        self._validate_shape_consistency()

    def _validate_shape_consistency(self):
        # 1. Calculate expected size from shape & dtype
        num_elements = 1
        for dim in self.shape:
            num_elements *= dim
        
        element_size = torch.tensor([], dtype=self.get_torch_dtype()).element_size()
        expected_bytes = num_elements * element_size
        
        # 2. Calculate actual size from offsets
        actual_bytes = self.offsets.get_diff()
        
        # 3. Assert they match
        if expected_bytes != actual_bytes:
            raise ValueError(
                f"Corrupted Tensor '{self.name}': "
                f"Shape claims {expected_bytes} bytes ({self.shape} x {self.dtype}), "
                f"but offsets reserve {actual_bytes} bytes."
            )

    def get_bytesize(self) -> int:
        return self.offsets.get_diff()

    def get_item_count(self) -> int:
        count = 1
        for dim in self.shape:
            count *= dim
        return count

    def get_torch_dtype(self) -> torch.dtype:
        # Handle unknown/unsupported dtypes
        if self.dtype not in safetenors_to_torch_dtype:
            raise ValueError(f"Unsupported dtype '{self.dtype}' in tensor '{self.name}'")
        return safetenors_to_torch_dtype[self.dtype]


class Offsets:
    def __init__(self, offsets: List[int]) -> None:
        self.start = offsets[0]
        self.end = offsets[1]

    def get_diff(self) -> int:
        return self.end - self.start


def prepare_request(
    fs: DistributedStreamer, paths: List[str], s3_credentials: Optional[S3Credentials]
) -> List[Tuple[str, int, List[SafetensorMetadata], List[int]]]:
    safetensors_metadatas = SafetensorsMetadata.from_files(fs, paths, s3_credentials)
    return [(
        safetensors_metadata.offset,
        safetensors_metadata.tensors_metadata,
        safetensors_metadata.read_sizes,
    ) for safetensors_metadata in safetensors_metadatas]


def create_torch_tensor(
    buffer: memoryview, tensor_metadata: SafetensorMetadata
) -> torch.tensor:
    if tensor_metadata.get_item_count() == 0:
        return torch.empty(tensor_metadata.shape, dtype=tensor_metadata.get_torch_dtype())

    tensor = buffer.view(tensor_metadata.get_torch_dtype())
    
    # Reshape the tensor to its final, correct shape.
    return tensor.view(tensor_metadata.shape)