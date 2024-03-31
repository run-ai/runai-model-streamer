from __future__ import annotations
import torch
import struct
import json
from typing import List, Tuple
from runai_streamer.file_streamer.file_streamer import FileStreamer


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
}


class SafetensorsMetadata:
    def __init__(self, blob: any, offset: int) -> None:
        self.offset = offset
        self.tensors_metadata = []
        self.tensor_sizes = []

        for name, safetensor_metadata in blob.items():
            if name != "__metadata__":
                tensor_metadata = SafetensorMetadata(name, safetensor_metadata)
                self.tensors_metadata.append(tensor_metadata)
                self.tensor_sizes.append(tensor_metadata.get_bytesize())
        self.tensors_metadata.sort(key=lambda x: x.offsets.start)

    @staticmethod
    def from_file(filename: str) -> SafetensorsMetadata:
        with FileStreamer() as fs:
            header_size_buffer = bytearray(SAFETENSORS_HEADER_BUFFER_SIZE)
            fs.read_file(filename, 0, header_size_buffer)
            header_size = struct.unpack(
                LITTLE_ENDIAN_LONG_LONG_STRUCT_FORMAT, header_size_buffer
            )[0]
            header_buffer = bytearray(header_size)
            fs.read_file(filename, SAFETENSORS_HEADER_BUFFER_SIZE, header_buffer)
            metadata = json.loads(header_buffer)
            return SafetensorsMetadata(
                metadata, header_size + SAFETENSORS_HEADER_BUFFER_SIZE
            )


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


def prepare_request(path: str) -> Tuple[int, List[SafetensorMetadata], List[int]]:
    safetensors_metadata = SafetensorsMetadata.from_file(path)
    return (
        safetensors_metadata.offset,
        safetensors_metadata.tensors_metadata,
        safetensors_metadata.tensor_sizes,
    )


def create_torch_tensor(
    buffer: memoryview, offset: int, tensor_metadata: SafetensorMetadata
) -> torch.tensor:
    tensor = torch.frombuffer(
        buffer,
        dtype=tensor_metadata.get_torch_dtype(),
        count=tensor_metadata.get_item_count(),
        offset=offset,
    )
    return tensor.view(tensor_metadata.shape)