## Using Run:ai Model Streamer

### Streaming

Start streaming models by creating a `SafetensorsStreamer` object that will serve as a context manager. The `SafetensorsStreamer` object opens OS threads that will read the tensors from the Safetensor file to the CPU memory. Closing the object destroys the threads. You can control the number of concurrent threads with the environment variable `RUNAI_STREAMER_CONCURRENCY`.

#### Streaming from a file system

If your SafeTensors file resides on a file system, run the following code to load the tensors to the CPU buffer and stream them to the GPU memory:

```python
from runai_model_streamer import SafetensorsStreamer

file_path = "/path/to/file.safetensors"

with SafetensorsStreamer() as streamer:
    streamer.stream_file(file_path)
    for name, tensor in streamer.get_tensors():
        tensor.to('CUDA:0')
```

> **Note:** To make the tensors available on the CPU memory, clone the yielded tensors before calling `streamer.get_tensors()`. Note that otherwise, tensors may be overwritten when using `RUNAI_STREAMER_MEMORY_LIMIT` or completely destroyed when closing the `SafetensorsStreamer` object.

#### Streaming from multiple files

To stream tensors from multiple files in parallel use the `streamer.stream_files()` API:

```python
from runai_model_streamer import SafetensorsStreamer

file_paths = ["/path/to/file-1.safetensors", "/path/to/file-2.safetensors"]

with SafetensorsStreamer() as streamer:
    streamer.stream_files(file_paths)
    for name, tensor in streamer.get_tensors():
        tensor.to('CUDA:0')
```

> **Note:** You can not mix S3 path and file system paths on same `streamer.stream_files()` call.

#### Distributed streaming

##### Use case and motivation

Distributed streaming is for multiple processes which are reading the same file list, e.g., the default loader of vLLM loading model weights on multiple devices.

When reading from a file system, the operating system page cache optimizes the reading by storing pages in the cache, so files are read from storage only once. However, reading from object storage cannot utilize the page cache, leading to multiple reads from storage and long loading times.

Distributed streaming is designed to solve this problem by dividing the reading workload between the multiple processes, where each process reads a unique portion of the files and then distributes its share to the other processes.

##### Usage

```python
from runai_model_streamer import SafetensorsStreamer

file_paths = ["/path/to/file-1.safetensors", "/path/to/file-2.safetensors"]

tensors = {}
device = 'CUDA:0'
with SafetensorsStreamer() as streamer:
    streamer.stream_files(file_paths, s3_credentials=None, device=device, is_distributed=True)
    for name, tensor in streamer.get_tensors():       
       tensors[name] = tensor.clone().detach() # returning tensors on the specified device, which is CUDA:0
```

##### Requirements

Distributed streaming allocates reusable staging buffers on each device, which hold the data of the yielded tensors
Therefore, the yielded tensor might be overwritten at the next iteration. If tensors are used outside the iterator loop, clone and detach the yielded tensor to save a copy.
 
The memory requirements for the staging buffers is twice the size of the largest tensor in the files

Distributed streaming is based on a torch distributed group and the broadcast operation.
The backend of the torch group must support the broadcast operation.

The performance gain depends on the type of backend and the communication between devices.
The nccl backend with nvlink between devices is most suitable for distributed streaming.

##### Control

Distributed streaming is enabled by default when streaming from object storage to CUDA devices.
It is possible to disable distributed streaming by setting `RUNAI_STREAMER_DIST=0`

It is possible to force distributed streaming for other cases by setting `RUNAI_STREAMER_DIST=1`

##### Global and local modes

In local mode, the processes on each node divide the entire workload among themselves.

In global mode, the workload is divided between all the processes on all the nodes.

For example, with 2 nodes and 16 processes (8 processes on each node), each process reads 1/8 of the workload in local mode and 1/16 in global mode.

The mode should be selected according to the communication speed between the nodes.
By default, distributed streaming is done in local mode.
To enable global mode, set `RUNAI_STREAMER_DIST_GLOBAL=1`.

#### Streaming from S3

> **Note:** Streaming models from S3 requires the installation of the streamer S3 package, as can be found [here](#s3CapabilityInstallation).

To load tensors from object storage, replace the file path in the code above with your S3 path, e.g.:

```python
file_path = "s3://my-bucket/my/file/path.safetensors"
```

#### S3 authentication

By default, the streamer performs authentication via boto3 to obtain credentials, which are then passed to the S3 client in `libstreamers3.so`.

This is the recommended way to load tensors from AWS S3 storage

###### HTTPS certificate

Custom certificates file can be passed using `AWS_CA_BUNDLE=path/to/ca_file`

The file path can also be configured in `~/.aws/config` file with `ca_bundle = /path/to/ca_file`, in case the authentication is via boto3 (`RUNAI_STREAMER_NO_BOTO3_SESSION=0` which is the default)

###### Authentication via AWS CPP SDK 

The boto3 authentication can be disabled by setting `RUNAI_STREAMER_NO_BOTO3_SESSION=1`.

In this case credentials can be passed in one of the following ways:

1. Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`

2. Credentials file `~/.aws/credentials`

3. Temporary credentials generated by AWS Security Token Service (STS) and passed as environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`

If IAM role assumption is needed, session token should be created using the AWS Security Token Service (STS).

e.g. `aws sts assume-role --role-arn arn:aws:iam::<account_name>:role/<role> --role-session-name ecs-session`

The session token shoud be passed as an environment variable `AWS_SESSION_TOKEN`

To check if IAM role assumption is needed run `aws s3 ls s3://your-bucket-name --region your-region`. If you get a `403 Forbidden` error, you might need an assumed role

#### Streaming from Azure Blob Storage

> **Note:** Streaming models from Azure Blob Storage requires the installation of the streamer Azure package, as can be found [here](#azureCapabilityInstallation).

To load tensors from Azure Blob Storage, replace the file path in the code above with your Azure path, e.g.:

```python
file_path = "az://my-container/my/file/path.safetensors"
```

##### Azure Authentication

The streamer uses Azure's DefaultAzureCredential for authentication, which provides a seamless authentication experience across development and production environments.

###### Default Azure Credential (Recommended)

Set the storage account name and DefaultAzureCredential handles authentication automatically:

```bash
export AZURE_STORAGE_ACCOUNT_NAME="myaccount"
```

The DefaultAzureCredential chain tries multiple authentication methods in order:
1. **Environment variables** (`AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`) - for service principal authentication
2. **Managed Identity** - no configuration needed when running in Azure (VMs, AKS, App Service, etc.)
3. **Azure CLI** - authenticate via `az login`
4. **Azure PowerShell** - authenticate via `Connect-AzAccount`
5. **Azure Developer CLI** - authenticate via `azd auth login`

See [DefaultAzureCredential](https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication#defaultazurecredential) for more information.

###### Service Principal Authentication

For automated pipelines and CI/CD, use service principal credentials:

```bash
export AZURE_STORAGE_ACCOUNT_NAME="myaccount"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

###### Managed Identity

When running in Azure (VMs, AKS, Azure Functions, etc.), managed identity is used automatically:

```bash
export AZURE_STORAGE_ACCOUNT_NAME="myaccount"
# No additional configuration needed - managed identity is detected automatically
```

###### Custom Endpoint (Private Endpoints)

For Azure Private Endpoints or custom storage endpoints, set the endpoint URL:

```bash
export AZURE_STORAGE_ENDPOINT="https://myaccount.privatelink.blob.core.windows.net"
```

When using a custom endpoint, `AZURE_STORAGE_ACCOUNT_NAME` is optional since the endpoint URL is used directly.

#### Streaming from Google cloud storage

##### SDK Authentication

GCS SDK backend is provided through the Python package `runai-model-streamer-gcs`.

To authentication to GCS, there are multiple configuration options:

1. External Credentials: If you set the `RUNAI_STREAMER_GCS_CREDENTIAL_FILE` environment variable, the
   SDK will load credentials from a JSON file at the path specified (eg: service account credentials)
2. Default Credentials: If you set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable, the google-cloud-cpp
   SDK will read application default credentials from a JSON file at the path specified.
3. Metadata Server: If neither above environment variables are set, the SDK attempts to fetch an auth token from
   the GCP metadata server. This is applicable when running on a GCE, GKE or GAE environment.

See [How Application Default Credentials works](https://cloud.google.com/docs/authentication/application-default-credentials)
for more information.

##### HMAC Authentication

S3 compatible HMAC authentication to GCS is provided through the Python package `runai-model-streamer-s3`.

To use HMAC credentials, you can use the S3 backend library, and AWS environment variables.
You should set the following variables:
 * `AWS_ACCESS_KEY_ID`: Set this to the Interoperability Access ID for your GCS bucket
 * `AWS_SECRET_ACCESS_KEY`: Set this to the Interoperability Secret for your GCS bucket
 * `AWS_ENDPOINT_URL`: Set this to `https://storage.googleapis.com`
 * `AWS_EC2_METADATA_DISABLED`: Set this to `true`

See [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys) for more information.

The streamer supports GCS URLs when using HMAC authentication
(eg: `gs://my-bucket/my/file/path.safetensors`).

#### Streaming from S3 compatible storage

To load tensors from S3 compatible object store, define the following environment variables

`RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING=0 AWS_ENDPOINT_URL="your_S3_endpoint" AWS_EC2_METADATA_DISABLED=true`

Setting the environment variable `AWS_ENDPOINT_URL` is mandatory

Setting the environment variable `AWS_EC2_METADATA_DISABLED` is needed in order to avoid a delay of few seconds, which happens only when the aws s3 sdk is used for compatible storage as explained [here](https://github.com/aws/aws-sdk-cpp/issues/1410)   

####  Troubleshooting

For the object storage SDK trace logs pass the environment variable `RUNAI_STREAMER_S3_TRACE=1` - this will create a log file in the location of the application

For the streamer internal logs pass the environment variables `RUNAI_STREAMER_LOG_TO_STDERR=1 RUNAI_STREAMER_LOG_LEVEL=DEBUG`

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
