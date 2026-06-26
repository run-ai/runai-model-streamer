import hashlib
import json
import os
import logging
import time
from typing import Dict, List, Optional, Tuple

from runai_model_streamer.s3_utils.s3_utils import (
    S3Credentials,
    is_s3_path,
    is_gs_path,
    is_azure_path,
)

logger = logging.getLogger(__name__)

RUNAI_STREAMER_CACHE_DIR_ENV = "RUNAI_STREAMER_CACHE_DIR"


def _is_object_storage_path(path: str) -> bool:
    return is_s3_path(path) or is_gs_path(path) or is_azure_path(path)


def _cache_key(remote_path: str, rank: int = 0, world_size: int = 1) -> str:
    """Deterministic cache filename from a remote URI and TP config."""
    h = hashlib.sha256(remote_path.encode()).hexdigest()[:16]
    basename = os.path.basename(remote_path.rstrip("/"))
    if world_size > 1:
        return f"{basename}.tp{world_size}_rank{rank}.{h}"
    return f"{basename}.{h}"


class _CacheWriter:
    """Incrementally writes streamed data to a cache file."""

    def __init__(self, cache_dir: str, remote_path: str, file_offset: int, rank: int = 0, world_size: int = 1) -> None:
        self._remote_path = remote_path
        self._file_offset = file_offset
        self._rank = rank
        self._world_size = world_size
        key = _cache_key(remote_path, rank, world_size)
        self._final_path = os.path.join(cache_dir, key)
        self._sentinel = self._final_path + ".done"
        self._tmp_path = self._final_path + f".partial.{os.getpid()}"
        self._fd = os.open(self._tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        self._written = 0
        self._start_time = time.time()

    def append(self, data) -> None:
        """Append data to the cache file. Accepts bytes, memoryview, or numpy array."""
        mv = memoryview(data).cast('B')
        total = len(mv)
        written = 0
        while written < total:
            n = os.write(self._fd, mv[written:])
            if n == 0:
                raise OSError(f"os.write returned 0, disk may be full")
            written += n
        self._written += total

    def finalize(self) -> None:
        try:
            os.close(self._fd)
            self._fd = -1
            # Another worker may have already written the final file
            if os.path.exists(self._final_path):
                logger.info(f"[RunAI Streamer][Cache] Already cached by another worker: {self._remote_path}")
                self._cleanup()
                return
            os.rename(self._tmp_path, self._final_path)
            meta = {"remote_path": self._remote_path, "file_offset": self._file_offset, "size": self._written, "rank": self._rank, "world_size": self._world_size}
            with open(self._sentinel, "w") as f:
                json.dump(meta, f)

            elapsed = time.time() - self._start_time
            throughput = self._written / elapsed / (1024 * 1024) if elapsed > 0 else 0
            logger.info(
                f"[RunAI Streamer][Cache] Cached: {self._remote_path} "
                f"({self._written} bytes) in {elapsed:.1f}s ({throughput:.0f} MB/s) "
                f"[rank={self._rank}, tp={self._world_size}]"
            )
        except OSError as e:
            # Race with another worker — if final file now exists, that's fine
            if os.path.exists(self._final_path):
                logger.info(f"[RunAI Streamer][Cache] Already cached by another worker: {self._remote_path}")
            else:
                logger.error(f"[RunAI Streamer][Cache] Finalize failed for {self._remote_path}: {e}")
            self._cleanup()

    def abort(self) -> None:
        self._cleanup()

    def _cleanup(self) -> None:
        if self._fd >= 0:
            try:
                os.close(self._fd)
            except OSError:
                pass
            self._fd = -1
        try:
            os.unlink(self._tmp_path)
        except OSError:
            pass


class StreamCache:
    """Write-through cache for object storage files.

    Writes streamed tensor data to local cache incrementally as batches complete.
    On subsequent loads, serves from local filesystem with offset=0 (the cached
    file contains only the tensor data, starting from the original file_offset).
    """

    def __init__(self, cache_dir: Optional[str] = None) -> None:
        self._cache_dir = (cache_dir or os.getenv(RUNAI_STREAMER_CACHE_DIR_ENV) or "").strip() or None
        self._writers: Dict[str, _CacheWriter] = {}
        self._cache_start_time: Optional[float] = None
        self._rank: int = 0
        self._world_size: int = 1

        if self._cache_dir:
            os.makedirs(self._cache_dir, exist_ok=True)
            logger.info(f"[RunAI Streamer][Cache] Cache enabled, directory: {self._cache_dir}")
        else:
            logger.info("[RunAI Streamer][Cache] Cache disabled (RUNAI_STREAMER_CACHE_DIR not set)")

    def set_distributed(self, rank: int, world_size: int) -> None:
        """Set the distributed rank and world size for cache key generation."""
        self._rank = rank
        self._world_size = world_size
        logger.info(f"[RunAI Streamer][Cache] Distributed config: rank={rank}, world_size={world_size}")

    @property
    def enabled(self) -> bool:
        return self._cache_dir is not None

    def cached_path_and_offset(self, remote_path: str) -> Optional[Tuple[str, int]]:
        """Return (local_path, adjusted_offset) if cached, else None.

        The cached file stores tensor data starting from the original file_offset,
        so the adjusted offset is always 0. Validates that the cached entry matches
        the current TP configuration.
        """
        if not self.enabled:
            return None
        if not _is_object_storage_path(remote_path):
            return None

        key = _cache_key(remote_path, self._rank, self._world_size)
        local_path = os.path.join(self._cache_dir, key)
        sentinel = local_path + ".done"

        data_exists = os.path.exists(local_path)
        sentinel_exists = os.path.exists(sentinel)

        if data_exists and sentinel_exists:
            # Validate sentinel metadata matches current TP config
            try:
                with open(sentinel, "r") as f:
                    meta = json.loads(f.read())
                cached_world_size = meta.get("world_size", 1)
                cached_rank = meta.get("rank", 0)
                if cached_world_size != self._world_size or cached_rank != self._rank:
                    logger.info(
                        f"[RunAI Streamer][Cache] STALE: {remote_path} cached with "
                        f"tp{cached_world_size}_rank{cached_rank} but current is "
                        f"tp{self._world_size}_rank{self._rank} — skipping"
                    )
                    return None
            except (json.JSONDecodeError, OSError):
                logger.info(f"[RunAI Streamer][Cache] INVALID: {remote_path} sentinel unreadable — treating as miss")
                return None

            file_size = os.path.getsize(local_path)
            logger.info(f"[RunAI Streamer][Cache] HIT: {remote_path} -> {local_path} ({file_size} bytes)")
            return local_path, 0

        if data_exists and not sentinel_exists:
            logger.info(f"[RunAI Streamer][Cache] INCOMPLETE: {local_path} exists but .done sentinel missing (partial download?)")
        else:
            logger.info(f"[RunAI Streamer][Cache] MISS: {remote_path} (not in cache)")

        return None

    def open_writer(self, remote_path: str, file_offset: int, total_bytes: int) -> None:
        """Open a cache writer for a file being streamed from object storage.

        Called once per file when streaming starts for a cache miss.
        """
        if not self.enabled:
            return
        if not _is_object_storage_path(remote_path):
            return
        if remote_path in self._writers:
            return
        if self.cached_path_and_offset(remote_path) is not None:
            return

        if self._cache_start_time is None:
            self._cache_start_time = time.time()

        logger.info(f"[RunAI Streamer][Cache] Opening cache writer for: {remote_path} ({total_bytes} bytes, rank={self._rank}, tp={self._world_size})")
        self._writers[remote_path] = _CacheWriter(self._cache_dir, remote_path, file_offset, self._rank, self._world_size)

    def append_data(self, remote_path: str, data: bytes) -> None:
        """Append streamed data to the cache file."""
        writer = self._writers.get(remote_path)
        if writer is None:
            return
        writer.append(data)

    def finalize(self, remote_path: str) -> None:
        """Finalize a cache file after all data has been written."""
        writer = self._writers.pop(remote_path, None)
        if writer is None:
            return
        writer.finalize()
        if not self._writers:
            elapsed = time.time() - self._cache_start_time if self._cache_start_time else 0
            logger.info(f"[RunAI Streamer][Cache] All files cached in {elapsed:.1f}s")

    def finalize_all(self) -> None:
        """Finalize all remaining cache writers."""
        for path in list(self._writers.keys()):
            self.finalize(path)

    def has_pending_writers(self) -> bool:
        """Return True if there are open cache writers."""
        return len(self._writers) > 0

    def abort_all(self) -> None:
        """Clean up incomplete writers on error."""
        for writer in self._writers.values():
            writer.abort()
        self._writers.clear()
