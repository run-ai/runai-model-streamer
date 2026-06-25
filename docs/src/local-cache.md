# Local Model Caching (RUNAI_STREAMER_CACHE_DIR)

## Overview

When streaming models from object storage (S3, GCS, Azure), the first load downloads tensor data over the network. With local caching enabled, this data is simultaneously written to a local NVMe directory. On subsequent loads, the streamer reads directly from local NVMe instead of re-downloading — significantly reducing model loading time.

## How to Enable

Set the environment variable:

```bash
export RUNAI_STREAMER_CACHE_DIR=/path/to/cache
```

For Kubernetes deployments, mount a fast local volume (NVMe-backed hostPath) and set the env:

```yaml
env:
  - name: RUNAI_STREAMER_CACHE_DIR
    value: "/models"
volumeMounts:
  - name: model-cache
    mountPath: /models
volumes:
  - name: model-cache
    hostPath:
      path: /mnt/k8s-disks/0/models
      type: DirectoryOrCreate
```

## How It Works

### First Load (Cache Miss)

```
S3 ──→ C++ streamer ──→ DRAM buffer ──→ GPU
                              │
                              ├──→ os.write() to /models/<file>.partial.<pid>
                              │    (inline, using buffer protocol — no extra copy)
                              │
                              └──→ when file complete: rename .partial → final, write .done sentinel
```

1. The streamer pulls tensor data from S3 as normal
2. After each batch is yielded to the model, the same buffer is written to a local `.partial` file
3. When all bytes for a file are written, it's atomically renamed to the final path and a `.done` sentinel is created
4. Model loading speed is nearly unchanged (NVMe writes are fast and mostly overlap with network wait)

### Second Load (Cache Hit)

```
/models/<cached_file> ──→ C++ streamer (local filesystem, NVMe) ──→ GPU
```

1. Before streaming, the cache checks if ALL files needed by this worker exist locally with valid `.done` sentinels
2. If all hit: swap S3 paths to local paths, set offsets to positions within the cached file
3. The C++ layer reads from local NVMe — no network access, full disk bandwidth (~5-7 GB/s)

## Cache Directory Structure

With distributed streaming (tensor parallelism), each worker caches its own partition:

```
/models/
├── model-00001-of-00026.safetensors.tp4_rank0.61cc262a76817ddb
├── model-00001-of-00026.safetensors.tp4_rank0.61cc262a76817ddb.done
├── model-00001-of-00026.safetensors.tp4_rank1.61cc262a76817ddb
├── model-00001-of-00026.safetensors.tp4_rank1.61cc262a76817ddb.done
├── model-00001-of-00026.safetensors.tp4_rank2.61cc262a76817ddb
├── model-00001-of-00026.safetensors.tp4_rank2.61cc262a76817ddb.done
├── model-00001-of-00026.safetensors.tp4_rank3.61cc262a76817ddb
├── model-00001-of-00026.safetensors.tp4_rank3.61cc262a76817ddb.done
├── model-00002-of-00026.safetensors.tp4_rank0.627ae2c16029180a
├── model-00002-of-00026.safetensors.tp4_rank0.627ae2c16029180a.done
└── ...
```

Without distributed streaming (TP=1), the TP prefix is omitted:

```
/models/
├── model-00001-of-00026.safetensors.61cc262a76817ddb
├── model-00001-of-00026.safetensors.61cc262a76817ddb.done
└── ...
```

### File naming format

```
<original_filename>.tp<world_size>_rank<rank>.<sha256_prefix_16chars>
```

- **original_filename**: basename from the S3 path (e.g., `model-00001-of-00026.safetensors`)
- **tp/rank**: tensor parallel configuration (omitted when world_size=1)
- **sha256 prefix**: 16-char hash of the full remote URI for uniqueness

### Sentinel file (.done)

Contains JSON metadata for validation:

```json
{
  "remote_path": "s3://bucket/model/model-00001-of-00026.safetensors",
  "file_offset": 8392,
  "size": 1073741824,
  "rank": 0,
  "world_size": 4
}
```

## Cache Hit Logic

A cache hit occurs when ALL of these conditions are met:

1. `RUNAI_STREAMER_CACHE_DIR` is set
2. `enable_cache=True` (only for tensor data streaming, not header reads)
3. For every unique file path in the request:
   - The cached data file exists on disk
   - The `.done` sentinel exists
   - The sentinel's `rank` and `world_size` match the current TP configuration

If **any** file misses, ALL files are streamed from remote (the C++ layer cannot mix local and remote paths in one request).

## Distributed Streaming (Tensor Parallelism)

With TP=4, each safetensors file contains many tensors. The distributed streamer partitions tensors across workers — each worker reads different tensor slices from every file. This means:

- Every worker reads from all 26 shard files, but different byte ranges
- Each worker caches only its own partition (identified by rank in the cache key)
- Cache entries for the same file but different ranks have different sizes
- Total cache size ≈ total model size (no duplication — partitions are non-overlapping)

On cache hit, cumulative offsets are computed so each `FileChunks` entry reads from the correct position within the cached file.

## Race Condition Handling

Multiple TP workers run as separate processes and may try to cache the same file:

- Each worker writes to its own `.partial.<pid>` file (unique per process)
- On finalize, if the final file already exists (another worker finished first), the worker silently cleans up its partial file
- No corruption, no errors — first writer wins

## Performance Characteristics

| Scenario | Typical Loading Time |
|----------|---------------------|
| S3 streaming (no cache) | ~15-18s |
| S3 streaming + cache write (first load) | ~16-18s |
| Cache hit (NVMe read, subsequent loads) | ~6-7s |
| NVMe preloaded baseline (for comparison) | ~6s |

The first-load overhead comes from inline `os.write()` calls. This is minimal because:
- NVMe write bandwidth is high (~5 GB/s)
- Writes use the buffer protocol (no Python-level copy)
- Files are finalized (fsync + rename) incrementally as each completes

## Logging

Enable logs with:

```bash
export RUNAI_STREAMER_LOG_LEVEL=INFO
```

Example output (first load):
```
[RunAI Streamer][Cache] Cache enabled, directory: /models
[RunAI Streamer][Cache] Distributed config: rank=0, world_size=4
[RunAI Streamer][Cache] MISS: s3://bucket/model-00001.safetensors (not in cache)
[RunAI Streamer][Cache] Cache miss for some files — streaming all 192 file(s) from remote
[RunAI Streamer][Cache] Opening cache writer for: s3://...model-00001... (1073741824 bytes, rank=0, tp=4)
[RunAI Streamer][Cache] Cached: s3://...model-00001... (1073741824 bytes) in 11.3s (90 MB/s)
[RunAI Streamer][Cache] All files cached in 11.5s
```

Example output (second load):
```
[RunAI Streamer][Cache] HIT: s3://...model-00001... -> /models/model-00001...tp4_rank0... (1073741824 bytes)
[RunAI Streamer][Cache] ALL 192 file(s) found in cache — using local paths (fast path)
[RunAI Streamer][Cache] read_device=cuda:0, _use_cuda_direct=False, all_cached_locally=True
```

## Concurrency Tuning

The `RUNAI_STREAMER_CONCURRENCY` (or `concurrency` in `model-loader-extra-config`) controls how many threads read from storage in parallel. This setting has a significant impact on cache hit performance:

| Concurrency | S3 (first load) | NVMe cache hit (second load) |
|-------------|-----------------|------------------------------|
| 64          | ~15s (optimal)  | ~17s (too many threads competing for NVMe) |
| 16-20       | ~17-18s         | ~8-9s (optimal for NVMe) |

**Why**: S3 benefits from high parallelism (many concurrent HTTP connections). NVMe benefits from fewer threads doing large sequential reads — too many threads cause seek contention and scheduler overhead.

**Recommendation**: Use `concurrency: 16` for optimal cache-hit performance. The first load from S3 will be slightly slower, but subsequent loads from NVMe will be significantly faster. If first-load speed is critical and you don't expect cache hits (e.g., always a new node), use higher concurrency.

```yaml
# Balanced for both S3 first-load and NVMe cache-hit
--model-loader-extra-config '{"concurrency": 16, "distributed": true}'
```

## Kernel Dirty Page Tuning (Recommended for Large Models)

On instances with limited RAM relative to model size (e.g., g6e.12xlarge with 96GB RAM loading a 64GB model), the default Linux `dirty_ratio` (20%) can cause `os.write()` to block during cache writes, significantly slowing down model loading.

**The problem:** With 4 TP workers each writing ~16GB to cache, total dirty pages reach 64GB — exceeding the default 20% limit (19GB). When the limit is hit, `os.write()` blocks until the kernel flushes pages to NVMe, stalling the streaming pipeline.

**The fix:** Increase `dirty_ratio` on the node so `os.write()` never blocks:

```bash
echo 80 > /proc/sys/vm/dirty_ratio
echo 50 > /proc/sys/vm/dirty_background_ratio
```

For Karpenter-managed nodes, add to the EC2NodeClass userData:

```yaml
userData: |
  MIME-Version: 1.0
  Content-Type: multipart/mixed; boundary="BOUNDARY"

  --BOUNDARY
  Content-Type: text/x-shellscript; charset="us-ascii"

  #!/bin/bash
  echo 80 > /proc/sys/vm/dirty_ratio
  echo 50 > /proc/sys/vm/dirty_background_ratio

  --BOUNDARY
  Content-Type: application/node.eks.aws

  ---
  apiVersion: node.eks.aws/v1alpha1
  kind: NodeConfig
  spec:
    featureGates:
      FastImagePull: true

  --BOUNDARY--
```

| Setting | Default | Recommended | Effect |
|---------|---------|-------------|--------|
| `dirty_ratio` | 20% | 80% | `os.write()` blocks only at 80% of RAM dirty |
| `dirty_background_ratio` | 10% | 50% | Kernel starts background flush at 50% (non-blocking) |

**Impact (g6e.12xlarge, 64GB model, TP=4):**

| | dirty_ratio=20% (default) | dirty_ratio=80% |
|---|---|---|
| Cache write overhead | +16.6s | +4.6s |
| First load with cache | 43.7s | 31.7s |
| Bottleneck | Kernel writeback stalls os.write | Physical NVMe write speed (unavoidable) |

This tuning is not needed on instances with large RAM (e.g., p5.48xlarge with 2TB) where dirty pages never approach the default limit.

## Limitations

- Cache is tied to the TP configuration — TP=4 cache cannot be reused by TP=8 (treated as a miss, new entries are created)
- All files must hit cache for the fast path; partial hits fall back to full S3 streaming
- Cache directory must be writable; use `DirectoryOrCreate` for hostPath volumes
- If the first pod's process dies before all files are finalized, some entries will be missing `.done` sentinels and treated as misses on next load
