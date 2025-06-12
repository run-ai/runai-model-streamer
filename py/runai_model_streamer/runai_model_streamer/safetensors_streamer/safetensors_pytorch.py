from __future__ import annotations
import torch
import struct
import json
from typing import List, Tuple
from runai_model_streamer.file_streamer.file_streamer import (FileStreamer, FileChunks)


SAFETENSORS_DATA_OFFSETS_KEY = "data_offsets"
SAFETENSORS_NAME_KEY = "name"
SAFETENSORS_SHAPE_KEY = "shape"
SAFETENSORS_DTYPE_KEY = "dtype"
SAFETENSORS_HEADER_BUFFER_SIZE = 8

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
                tensor_metadata = SafetensorMetadata(name, safetensor_metadata)
                self.tensors_metadata.append(tensor_metadata)

        self.tensors_metadata.sort(key=lambda x: x.offsets.start)

        for i in range(len(self.tensors_metadata)):
            if i < len(self.tensors_metadata) - 1:
                current_start = self.tensors_metadata[i].offsets.start
                next_start = self.tensors_metadata[i + 1].offsets.start
                self.read_sizes.append(next_start - current_start)
            else:
                self.read_sizes.append(self.tensors_metadata[i].get_bytesize())

    @staticmethod
    def from_files(fs: FileStreamer, filenames: str) -> List[SafetensorsMetadata]:
        fs.stream_files([FileChunks(filename, 0, [SAFETENSORS_HEADER_BUFFER_SIZE]) for filename in filenames])
        header_sizes = {}
        for file_path, ready_chunk_index, buffer in fs.get_chunks():
            header_sizes[file_path] = struct.unpack(
                LITTLE_ENDIAN_LONG_LONG_STRUCT_FORMAT, buffer
            )[0]
            
        metadatas = {}
        fs.stream_files([FileChunks(filename, SAFETENSORS_HEADER_BUFFER_SIZE, [header_size]) for filename, header_size in header_sizes.items()])
        for file_path, ready_chunk_index, buffer in fs.get_chunks():
            metadatas[file_path] = json.loads(bytearray(buffer))

        return [SafetensorsMetadata(
            metadatas[filename], header_sizes[filename] + SAFETENSORS_HEADER_BUFFER_SIZE
        ) for filename in filenames]


class SafetensorMetadata:
    def __init__(self, name: str, safetensorMetadata: any) -> None:
        self.name = name
        self.shape = safetensorMetadata[SAFETENSORS_SHAPE_KEY]
        self.dtype = safetensorMetadata[SAFETENSORS_DTYPE_KEY]
        self.offsets = Offsets(safetensorMetadata[SAFETENSORS_DATA_OFFSETS_KEY])

    def get_bytesize(self) -> int:
        return self.offsets.get_diff()

    def get_item_count(self) -> int:
        count = 1
        for dim in self.shape:
            count *= dim
        return count

    def get_torch_dtype(self) -> torch.dtype:
        return safetenors_to_torch_dtype[self.dtype]


class Offsets:
    def __init__(self, offsets: List[int]) -> None:
        self.start = offsets[0]
        self.end = offsets[1]

    def get_diff(self) -> int:
        return self.end - self.start


def prepare_request(
    fs: FileStreamer, paths: str
) -> List[Tuple[str, int, List[SafetensorMetadata], List[int]]]:
    safetensors_metadatas = SafetensorsMetadata.from_files(fs, paths)
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
    
    tensor = torch.frombuffer(
        buffer,
        dtype=tensor_metadata.get_torch_dtype(),
        count=tensor_metadata.get_item_count(),
        offset=0,
    )
    return tensor.view(tensor_metadata.shape)
