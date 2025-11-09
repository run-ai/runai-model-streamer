import os
import glob
import shutil
import fnmatch  # Still needed as a fallback/check
from pathlib import Path
from typing import List, Optional, Iterator, Any

import torch

# --- Import REAL implementations ---
from runai_model_streamer import (
    SafetensorsStreamer as OriginalSafetensorsStreamer,
    list_safetensors as original_list_safetensors,
)
from runai_model_streamer.s3_utils.s3_utils import (
    is_s3_path,
    is_gs_path,
    S3Credentials,
    filter_allow,
    filter_ignore,
    removeprefix,
)

class StreamerPatcher:
    """
    Generates mock objects for runai_model_streamer that redirect
    S3/GS paths to a local filesystem path.
    """

    def __init__(self, local_model_path: str):
        self.local_path = local_model_path
        print(f"[Patcher] Initialized with local path: {self.local_path}")

    def _is_remote_path(self, path: str) -> bool:
        """Helper to check if a path is S3 or GS."""
        return is_s3_path(path) or is_gs_path(path)

    # === Shim for list_safetensors ===
    def shim_list_safetensors(self, path: str, s3_credentials: Optional[S3Credentials] = None) -> List[str]:
        rewritten_path = self.local_path if self._is_remote_path(path) else path
        if rewritten_path != path:
            print(f"[SHIM] list_safetensors: Rewrote '{path}' to '{rewritten_path}'")
        return original_list_safetensors(rewritten_path, s3_credentials)

    # === Shim for pull_files ===
    def shim_pull_files(self, model_path: str,
                          dst: str,
                          allow_pattern: Optional[List[str]] = None,
                          ignore_pattern: Optional[List[str]] = None,
                          s3_credentials: Optional[S3Credentials] = None) -> None:
        
        # 1. Check if it's a path we're mocking (S3 or GS)
        if not (is_s3_path(model_path) or is_gs_path(model_path)):
            # Match the original's behavior for non-remote paths
            raise NotImplementedError("pull files is not implemented for file system paths")
        
        source_dir = os.path.normpath(self.local_path)
        print(f"[SHIM] pull_files: Simulating download from '{model_path}' (using local: '{source_dir}') to '{dst}'")

        # 2. Simulate `list_objects_v2` / `list_blobs` by walking the local dir
        # We need to generate a list of relative paths, just like S3/GS keys
        all_local_files_relative = []
        for root, dirs, files in os.walk(source_dir, topdown=True):
            # Filter out directories
            dirs[:] = [d for d in dirs if not (os.path.join(root, d) + "/").endswith("/")]
            
            for file in files:
                full_path = os.path.join(root, file)
                # Get the path relative to the model root
                relative_path = os.path.relpath(full_path, source_dir)
                # S3/GS keys use forward slashes
                all_local_files_relative.append(relative_path.replace(os.path.sep, '/'))

        # 3. --- REUSE ORIGINAL FILTERING LOGIC ---
        # This is the exact filtering logic from both S3 and GCS `list_files`
        
        print(f"[SHIM] pull_files: Filtering {len(all_local_files_relative)} local files...")
        
        # paths = _filter_ignore(paths, ["*/"])
        filtered_paths = filter_ignore(all_local_files_relative, ["*/"])
        
        # if allow_pattern is not None: paths = _filter_allow(paths, allow_pattern)
        if allow_pattern:
            filtered_paths = filter_allow(filtered_paths, allow_pattern)
            
        # if ignore_pattern is not None: paths = _filter_ignore(paths, ignore_pattern)
        if ignore_pattern:
            filtered_paths = filter_ignore(filtered_paths, ignore_pattern)

        if not filtered_paths:
            print("[SHIM] pull_files: No files matched patterns.")
            return

        print(f"[SHIM] pull_files: Matched {len(filtered_paths)} files.")

        # 4. Simulate the copy loop from `pull_files`
        # The `base_dir` in S3/GS-land is the prefix. In our case, the
        # `base_dir` is effectively empty string, because our
        # `filtered_paths` are already relative to the root.
        
        base_dir = "" # Our local paths are already relative
        
        for file in filtered_paths:
            # S3/GS logic: destination_file = os.path.join(dst, removeprefix(file, base_dir).lstrip("/"))
            relative_file = removeprefix(file, base_dir).lstrip("/")
            
            # Source path on local disk
            source_file_path = os.path.join(source_dir, relative_file.replace('/', os.path.sep))
            
            # Destination path
            destination_file = os.path.join(dst, relative_file.replace('/', os.path.sep))

            # Create parent directory in destination
            local_dir = Path(destination_file).parent
            os.makedirs(local_dir, exist_ok=True)
            
            # Copy the file
            print(f"[SHIM] pull_files: Copying {source_file_path} to {destination_file}")
            shutil.copy(source_file_path, destination_file)

    # === Shim for SafetensorsStreamer ===
       
    def create_mock_streamer(self, *args, **kwargs):
        return self.MockSafetensorsStreamer(self.local_path)

    class MockSafetensorsStreamer:
        def __init__(self, local_path: str):
            self.local_path = local_path
            self.original_streamer = OriginalSafetensorsStreamer()

        def _rewrite_file_path(self, path: str) -> str:
            if is_s3_path(path) or is_gs_path(path):
                # Assumes the path is a file, e.g., s3://.../model.safetensors
                return os.path.join(self.local_path, os.path.basename(path))
            return path

        def __enter__(self) -> "MockSafetensorsStreamer":
            self.original_streamer.__enter__()
            return self

        def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
            return self.original_streamer.__exit__(exc_type, exc_value, traceback)

        def stream_file(self, path: str, s3_credentials: Optional[S3Credentials] = None,
                          device: Optional[str] = "cpu", is_distributed: bool = False) -> None:
            rewritten_path = self._rewrite_file_path(path)
            if rewritten_path != path:
                print(f"[SHIM] stream_file: Rewriting '{path}' to '{rewritten_path}'")
            return self.original_streamer.stream_file(
                rewritten_path, s3_credentials, device, is_distributed
            )

        def stream_files(self, paths: List[str], s3_credentials: Optional[S3Credentials] = None,
                           device: Optional[str] = "cpu", is_distributed: bool = False) -> None:
            rewritten_paths = [self._rewrite_file_path(p) for p in paths]
            if rewritten_paths != paths:
                print(f"[SHIM] stream_files: Rewriting {paths} to {rewritten_paths}")
            return self.original_streamer.stream_files(
                rewritten_paths, s3_credentials, device, is_distributed
            )

        def get_tensors(self) -> Iterator[torch.tensor]:
            return self.original_streamer.get_tensors()