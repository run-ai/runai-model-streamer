## Environment Variables

### RUNAI_STREAMER_CONCURRENCY

Controls the level of concurrency and number of OS threads reading tensors from the file to the CPU buffer.

#### Values accepted

Positive integer value

#### Default value

20

### RUNAI_STREAMER_BLOCK_BYTESIZE

Controls the maximum size of memory each OS thread reads from the file at once.

#### Values accepted

Positive integer value

#### Default value

2097152 (=2MiB)

### RUNAI_STREAMER_CHUNK_BYTESIZE

Controls how much memory each thread reads from storage before checking if tensors are read in full and are ready to be moved from the CPU buffer to GPU memory. Low value means threads checking often if tensors are ready to be moved to GPU memory, potentially slowing down the read throughput from storage to CPU. High value means threads checking rarely if tensors are ready to be moved to GPU memory, potentially stalling the copy operation from CPU to GPU.

#### Values accepted

Positive integer value

#### Default value

8388608 (=8MiB)

### RUNAI_STREAMER_MEMORY_LIMIT

Controls how the CPU Memory buffer to which tensors are read from the file is being limited. Read more about it [here](#MemoryCapModesSection).

#### Values accepted

`-1 - UNLIMITED`, `0 - MIN`, or `Positive integer value - LIMITED`

#### Default value

`-1 - UNLIMITED`
