## Environment Variables

### RUNAI_STREAMER_DIRECTIO

Enables Direct I/O (O_DIRECT) when opening files, bypassing the kernel page cache.

This is useful to avoid double-caching effects when using network filesystems (NFS, SMB) or when the application already has its own caching layer. Direct I/O requires aligned buffers and read sizes, which are automatically handled by the streamer's block-based reading mechanism.

> [!WARNING]
> Direct I/O may not be supported on all filesystems. If the filesystem does not support O_DIRECT, file opening may fail or fall back to standard I/O depending on the system.

#### Values accepted

`1` to enable Direct I/O, any other value or unset to use standard I/O

#### Default value

Unset (standard I/O with kernel page cache)

### RUNAI_STREAMER_CONCURRENCY

Controls the level of concurrency and number of OS threads reading tensors from the file to the CPU buffer.

#### Values accepted

Positive integer value

#### Default value

16 for reading from file system

8 for reading from object storage

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

`-1` for distributed streaming and 40 GB otherwise

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

### RUNAI_STREAMER_GCS_CREDENTIAL_FILE

Specifies the path to a credential file to use for GCS authentication.

If not defined (default) Application Default Credentials are used.

#### Values accepted

String

### AZURE_STORAGE_ACCOUNT_NAME

Azure Storage account name. Used with DefaultAzureCredential for authentication.

#### Values accepted

String

#### Default value

None

### RUNAI_STREAMER_DIST

Enables distributed streaming for multiple devices

`auto` enables distributed streaming only for streaming from object storage to CUDA devices

### Values accepted

String `0` or `1` or `auto`

#### Default value

`auto`

### RUNAI_STREAMER_DIST_GLOBAL

Enables global distributed streaming for multiple devices, dividing the workload between multiple nodes

If not defined (default) distributed streaming is local - dividing the workload between processes of the same node 

### Values accepted

String `0` or `1`

#### Default value

`0`
