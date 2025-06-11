## Environment Variables

### RUNAI_STREAMER_CONCURRENCY

Controls the level of concurrency and number of OS threads reading tensors from the file to the CPU buffer.

#### Values accepted

Positive integer value

#### Default value

16

### RUNAI_STREAMER_CHUNK_BYTESIZE

Controls the maximum size of memory each OS thread reads from the file at once.

#### Values accepted

Positive integer value

> [!NOTE]
> When reading from file system the minimum size is 2097152 (=2MiB)
>
> When reading from object store the minimum size is 5242880 (=5MiB)

#### Default value

2097152 (=2MiB) when reading from file system

8388608 (=8MiB) when reading from object store e.g. S3

### RUNAI_STREAMER_MEMORY_LIMIT

Controls how the CPU Memory buffer to which tensors are read from the file is being limited. Read more about it [here](usage.md#cpu-memory-capping).

#### Values accepted

`-1 - UNLIMITED`, `0 - MIN`, or `Positive integer value - LIMITED`

#### Default value

`-1 - UNLIMITED`

### AWS_ENDPOINT_URL

Overrides url endpoint for reading from S3 compatible object store

> [!NOTE]
> 
> Mandatory for S3 compatible e.g. gcs, minio
> 
> Optional if reading from AWS S3

#### Values accepted

String

#### Default value

Default S3 url endpoint

### AWS_CA_BUNDLE

Specifies the path to a certificate bundle to use for HTTPS certificate validation.

If defined, this environment variable overrides the value for the profile setting ca_bundle.

#### Values accepted

String

### RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING

Controls parsing the url endpoint for reading from object store 

> [!NOTE]
> Optional - No need to set this variable for reading from S3
> 
> Should be `0` for reading from compatible object store

#### Values accepted

Boolean `0` or `1`

#### Default value

`1`
