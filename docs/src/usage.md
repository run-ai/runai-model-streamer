## Using Run:ai Model Streamer

### Streaming

Start streaming models by creating a `SafetensorsStreamer` object that will serve as a context manager. The `SafetensorsStreamer` object opens OS threads that will read the tensors from the Safetensor file to the CPU memory. Closing the object destroys the threads. You can control the number of concurrent threads with the environment variable `RUNAI_STREAMER_CONCURRENCY`.

#### Streaming from a file system

If your SafeTensors file resides on a file system, run the following code to load the tensors to the CPU buffer and stream them to the GPU memory:

```python
from runai_model_streamer.safetensors_streamer.safetensors_streamer import SafetensorsStreamer

file_path = "/path/to/file.safetensors"

with SafetensorsStreamer() as streamer:
    streamer.stream_file(file_path)
    for name, tensor in streamer.get_tensors():
        tensor.to('CUDA:0')
```

> **Note:** To make the tensors available on the CPU memory, clone the yielded tensors before calling `streamer.get_tensors()`. Note that otherwise, tensors may be overwritten when using `RUNAI_STREAMER_MEMORY_LIMIT` or completely destroyed when closing the `SafetensorsStreamer` object.

#### Streaming from S3

> **Note:** Streaming models from S3 requires the installation of the streamer S3 package, as can be found [here](#s3CapabilityInstallation).

To load tensors from object storage, replace the file path in the code above with your S3 path, e.g.:

```python
file_path = "s3://my-bucket/my/file/path.safetensors"
```

### CPU Memory Capping

The streamer allocates a buffer on the CPU Memory for storing the tensors before moving them to the GPU Memory. Control the size of the allocated buffer by using the environment variable `RUNAI_STREAMER_MEMORY_LIMIT`.

#### Unlimited CPU Memory

`RUNAI_STREAMER_MEMORY_LIMIT=-1`

The default value. The size of the allocated CPU Memory buffer is equal to the size of the safetensor file (without the file header) and there is no memory reuse between multiple `get_tensors()` requests. Use this option for maximum performance and fastest model streaming times.

#### Min

`RUNAI_STREAMER_MEMORY_LIMIT=0`

The size of allocated CPU memory is minimal and is equal to the size of the largest tensor in the file. The buffer is reused between all `get_tensors()` requests.

#### Limited

`RUNAI_STREAMER_MEMORY_LIMIT=NUMBER`

Use this option to control the size of the buffer by setting the value to a specific memory size. For example, limit the buffer to 4GB by setting `RUNAI_STREAMER_MEMORY_LIMIT=4000000000`. In this case, the streamer reuses the buffer memory between multiple `get_tensors()` requests.

> **Warning:** You cannot limit the buffer to sizes smaller than the size of the largest tensor in the file.
