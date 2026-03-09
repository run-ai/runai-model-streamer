from __future__ import annotations
from typing import Iterator, Optional
import torch
import glob
import os
import fcntl
import shutil
from typing import List

from runai_model_streamer.file_streamer import FileChunks

from runai_model_streamer.distributed_streamer import DistributedStreamer

import runai_model_streamer.safetensors_streamer.safetensors_pytorch as safetensors_pytorch

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    is_gs_path,
    is_azure_path,
    s3_glob,
    s3_pull_files,
    gcs_glob,
    gcs_pull_files,
    azure_glob,
    azure_pull_files,
)

SAFETENSORS_PATTERN = "*.safetensors"

def list_safetensors(path: str, s3_credentials : Optional[S3Credentials] = None) -> List[str]:
    """
    List all safetensors files in the given path.
    This function is not recursive.
    """
    if is_s3_path(path):
        files = s3_glob(path, [SAFETENSORS_PATTERN], s3_credentials)
    elif is_gs_path(path):
        files = gcs_glob(path, [SAFETENSORS_PATTERN])
    elif is_azure_path(path):
        files = azure_glob(path, [SAFETENSORS_PATTERN])
    else:
        files = glob.glob(os.path.join(path, SAFETENSORS_PATTERN))
    
    return files

def pull_files(model_path: str,
                dst: str,
                allow_pattern: Optional[List[str]] = None,
                ignore_pattern: Optional[List[str]] = None,
                s3_credentials : Optional[S3Credentials] = None) -> None:
    """
    Pull all safetensors files in the given path.
    This function is recursive.
    """
    if is_s3_path(model_path):
        return s3_pull_files(model_path, dst, allow_pattern, ignore_pattern, s3_credentials)
    if is_gs_path(model_path):
        return gcs_pull_files(model_path, dst, allow_pattern, ignore_pattern)
    if is_azure_path(model_path):
        return azure_pull_files(model_path, dst, allow_pattern, ignore_pattern)
    raise NotImplementedError("pull files is not implemented for file system paths")

class ObjectStorageModel:
    """
    Process-safe, idempotent wrapper for downloading model files from object storage.

    Multiple processes calling pull_files() concurrently with the same dst will
    serialize via a file lock. The first process downloads; the rest wait and skip
    (sentinel-based idempotency).

    Use as a context manager. The sentinel is written and the lock is released on
    clean exit. If an exception is raised inside the block the lock is still released
    but the sentinel is NOT written, so the next process will retry the download.

    Locking mechanism:
        Uses fcntl.flock, which is supported on Linux only (not Windows).
        The lock file is placed at dst + ".lock".

        Supported: multiple processes on the same machine, whether dst is a local
        disk or a network-mounted filesystem (NFS, EFS, etc.) — the kernel manages
        the lock locally.

        Not supported: processes on different machines sharing the same dst over
        a network filesystem. fcntl.flock does not provide reliable cross-host
        locking on NFS (NFSv3 and earlier silently ignore it; NFSv4 depends on
        mount options and server support).

    Example::

        with ObjectStorageModel(model_path=url, dst=cache_dir) as obj:
            obj.pull_files(allow_pattern=["*.safetensors"])
            obj.pull_files(ignore_pattern=["*.safetensors"])
    """

    SENTINEL_NAME = ".runai_complete"

    def __init__(
        self,
        model_path: str,
        dst: str,
        s3_credentials: Optional[S3Credentials] = None,
    ) -> None:
        if not (is_s3_path(model_path) or is_gs_path(model_path) or is_azure_path(model_path)):
            raise ValueError(
                f"model_path {model_path!r} is not a supported object storage path "
                "(expected s3://, gs://, or az://)"
            )
        self.dir = dst
        self._model_path = model_path if model_path.endswith("/") else model_path + "/"
        self._s3_credentials = s3_credentials
        self._lock_path = dst + ".lock"
        self._sentinel = os.path.join(dst, self.SENTINEL_NAME)
        self._lock_file = open(self._lock_path, "w")
        fcntl.flock(self._lock_file, fcntl.LOCK_SH)  # shared: fast path for already-downloaded
        try:
            if os.path.exists(self._sentinel):
                self._skip = True
            else:
                # Upgrade to exclusive before modifying dst.
                # flock upgrade is not atomic: re-check sentinel after acquiring EX
                # because another process may have completed the download in the gap.
                fcntl.flock(self._lock_file, fcntl.LOCK_EX)
                if os.path.exists(self._sentinel):
                    self._skip = True
                else:
                    self._skip = False
                    if os.path.exists(dst):
                        shutil.rmtree(dst)
                    os.makedirs(dst, exist_ok=True)
        except BaseException:
            # BaseException (not Exception) is intentional: KeyboardInterrupt and
            # SystemExit must also release the lock, otherwise sibling processes
            # waiting on flock will block indefinitely after a Ctrl+C or SIGTERM.
            fcntl.flock(self._lock_file, fcntl.LOCK_UN)
            self._lock_file.close()
            raise

    def pull_files(
        self,
        allow_pattern: Optional[List[str]] = None,
        ignore_pattern: Optional[List[str]] = None,
    ) -> None:
        if self._skip:
            return
        pull_files(self._model_path, self.dir, allow_pattern, ignore_pattern, self._s3_credentials)

    def __enter__(self) -> ObjectStorageModel:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        try:
            if exc_type is None and not self._skip:
                downloaded = [f for f in os.listdir(self.dir) if f != self.SENTINEL_NAME]
                if not downloaded:
                    raise RuntimeError(
                        f"No files were downloaded to {self.dir!r} — "
                        "verify that the model path is correct"
                    )
                try:
                    with open(self._sentinel, "w"):
                        pass
                except OSError as exc:
                    raise RuntimeError(
                        f"Failed to write download sentinel {self._sentinel!r}: {exc}"
                    ) from exc
        finally:
            try:
                fcntl.flock(self._lock_file, fcntl.LOCK_UN)
            finally:
                self._lock_file.close()
        return False


class SafetensorsStreamer:
    def __init__(self) -> None:
        self.file_streamer = DistributedStreamer()
        self.files_to_tensors_metadata = {}

    def __enter__(self) -> SafetensorsStreamer:
        self.file_streamer.__enter__()
        return self

    def __exit__(self, exc_type: any, exc_value: any, traceback: any) -> None:
        return self.file_streamer.__exit__(exc_type, exc_value, traceback)

    def stream_file(
            self,
            path: str,
            s3_credentials : Optional[S3Credentials] = None,
            device: Optional[str] = "cpu",
            is_distributed: bool = False,
        ) -> None:
        return self.stream_files([path], s3_credentials, device, is_distributed)

 
    def stream_files(
            self,
            paths: List[str],
            s3_credentials : Optional[S3Credentials] = None,
            device: Optional[str] = "cpu",
            is_distributed: bool = False, 
        ) -> None:
        self.files_to_tensors_metadata = {}

        file_stream_requests: List[FileChunks] = []

        # metadata is created on cpu and each process reads it individually
        safetensors_metadatas = safetensors_pytorch.prepare_request(self.file_streamer, paths, s3_credentials)

        for i in range(len(paths)):
            (file_offset, tensors_metadata, tensor_sizes) = safetensors_metadatas[i]
            path = paths[i]
            self.files_to_tensors_metadata[i] = tensors_metadata
            file_stream_requests.append(FileChunks(i, path, file_offset, tensor_sizes))

        self.file_streamer.stream_files(
            file_stream_requests,
            credentials=s3_credentials,
            device=device,
            is_distributed=is_distributed,
        )

    def get_tensors(self) -> Iterator[torch.tensor]:
        for file_index, ready_chunk_index, buffer in self.file_streamer.get_chunks():
            tensor_metadata = self.files_to_tensors_metadata[file_index][ready_chunk_index]
            yield tensor_metadata.name, safetensors_pytorch.create_torch_tensor(
                buffer, tensor_metadata
            )
