## Environment Variables

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

### RUNAI_STREAMER_S3_UNSIGNED

Enables unsigned (anonymous) requests to S3. Use this when accessing public S3 buckets that do not require authentication.

#### Values accepted

Boolean `0` or `1`

#### Default value

`0`

### RUNAI_STREAMER_S3_MAX_RETRIES

Overrides the AWS S3 CRT retry limit for each ranged read request. Retries use
exponential backoff with full jitter and are attempted only for failures that the
AWS CRT classifies as retryable. The initial request is not included in this
number; for example, `3` permits up to four total attempts.

Set this to `0` to allow the initial AWS request but disable AWS CRT retry
attempts. If the variable is not set, the AWS S3 CRT client's native retry
policy is used. Once that policy is exhausted, a
terminal error that AWS still classifies as retryable can enter Run:ai's
application-level chunk retry loop when `RUNAI_STREAMER_TIMEOUT` is enabled.

This limit multiplies the time allowed by `RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS`
inside one application attempt. `RUNAI_STREAMER_TIMEOUT` remains the total
deadline across any additional Run:ai attempts.

#### Values accepted

Non-negative integer

#### Default value

AWS S3 CRT default retry policy

### RUNAI_STREAMER_TIMEOUT

Controls the total application-level object-storage retry budget for one
streaming submission. The value is measured in seconds. All chunks in the
submission share one absolute deadline.

After the storage plugin's native retry policy is exhausted, only the failed
`ObjectChunk` is requeued with exponential full-jitter backoff. Chunks that have
already completed are preserved. When the deadline expires, the failed chunk is
reported as `FileAccessError`.

For S3, application retries are attempted for transport failures (no HTTP
response), HTTP 5xx, 408 and 429, or another error the AWS SDK still marks
retryable. Permanent client errors such as HTTP 400, 401, 403 and 404 are failed
immediately. GCS and Azure do not currently emit the internal retryable
completion marker, so this setting does not add retries for those plugins.

Set this to `0` or leave it unset to preserve fail-fast behavior after the
storage plugin's retry policy is exhausted.

#### Values accepted

Non-negative integer, in seconds

#### Default value

`0` (application-level chunk retries disabled)

### RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS

Controls how long an S3 connection may remain below
`RUNAI_STREAMER_S3_LOW_SPEED_LIMIT` before the current attempt fails. This is a
low-throughput timeout, not an overall deadline for the complete model download.
A retry starts a new attempt with a new timeout interval.

The S3 CRT enforces a minimum monitoring interval of 3 seconds.

#### Values accepted

Non-negative integer, in milliseconds. `0` leaves the AWS SDK setting unchanged.

#### Default value

`1000` (the effective S3 CRT monitoring interval is at least 3 seconds)

### RUNAI_STREAMER_S3_LOW_SPEED_LIMIT

Controls the minimum acceptable S3 transfer rate. If the connection remains
below this rate for `RUNAI_STREAMER_S3_REQUEST_TIMEOUT_MS`, the attempt fails and
the retry policy decides whether to try again.

#### Values accepted

Non-negative integer, in bytes per second. `0` leaves the AWS SDK setting unchanged.

#### Default value

AWS SDK default (`1` byte per second)

### RUNAI_STREAMER_GCS_CREDENTIAL_FILE

Specifies the path to a credential file to use for GCS authentication.

If not defined (default) Application Default Credentials are used.

#### Values accepted

String

### AZURE_STORAGE_ACCOUNT_NAME

Azure Storage account name. Required for all Azure Blob Storage authentication methods.

#### Values accepted

String

#### Default value

None

### AZURE_STORAGE_SAS_TOKEN

Shared Access Signature (SAS) token for Azure Blob Storage authentication. Used with AZURE_STORAGE_ACCOUNT_NAME.

The value should be the query string portion of the SAS URI (with or without leading `?`), e.g. `sv=2021-08-06&ss=b&srt=co&sp=rl&se=...&sig=...`

#### Values accepted

String

#### Default value

None

### AZURE_STORAGE_ENDPOINT_SUFFIX

Azure Blob Storage endpoint suffix. Override for sovereign clouds (China, US Government) or Azure Stack.

#### Values accepted

String (e.g. `blob.core.chinacloudapi.cn`, `blob.core.usgovcloudapi.net`)

#### Default value

`blob.core.windows.net`

### RUNAI_STREAMER_EXPERIMENTAL_AZURE_CACHE_ENABLED

> **Experimental** — This feature is under active development and may change in future releases.

Controls whether the Azure Blob cache provider is enabled. When a compatible cache provider package (e.g., `tachyon-client`) is installed alongside `runai-model-streamer`, the streamer auto-discovers and loads it at runtime to accelerate model loading from Azure Blob Storage via a distributed cache.

Set to `0` to disable the cache provider entirely, even if the package is installed. This is the recommended way to disable caching in case of issues.

See [Azure Blob Cache Provider (Experimental)](usage.md#azure-blob-cache-provider-experimental) in the usage guide for details.

#### Values accepted

`0`, `1`, `auto`

#### Default value

Unset — cache is auto-enabled when a compatible cache provider package is installed

### RUNAI_STREAMER_DIST

Enables distributed streaming for multiple devices

`auto` enables distributed streaming only when using the `nccl` distributed backend

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

### RUNAI_STREAMER_GCS_USE_GRPC

Enables the gRPC transport for the GCS client, which utilizes direct connectivity for higher throughput and lower latency when running within Google Cloud.

If not defined (default) the GCS client uses the standard HTTP/JSON transport.

#### Values accepted

String `0` or `1`

#### Default value

`0`
