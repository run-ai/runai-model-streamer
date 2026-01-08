from typing import Optional, List, Tuple
from runai_model_streamer_azure.credentials.credentials import create_blob_service_client, AzureCredentials

import fnmatch
import os
import posixpath
from pathlib import Path

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    raise ImportError(
        "Azure Storage packages are not installed. "
        "Install them with: pip install azure-storage-blob azure-identity"
    )


def _create_client(credentials: Optional[AzureCredentials] = None) -> BlobServiceClient:
    return create_blob_service_client(credentials)


def glob(path: str, allow_pattern: Optional[List[str]] = None, credentials: Optional[AzureCredentials] = None) -> List[str]:
    """
    List files in Azure Blob Storage matching the given pattern.
    
    Args:
        path: Azure blob path in format "az://container/prefix"
        allow_pattern: Optional list of glob patterns to include
        credentials: Optional AzureCredentials object
        
    Returns:
        List of full Azure blob paths
    """
    client = _create_client(credentials)

    if not path.endswith("/"):
        path = f"{path}/"
    
    container_name, _, keys = list_files(client, path, allow_pattern)
    return [f"az://{container_name}/{key}" for key in keys]


def pull_files(
    model_path: str,
    dst: str,
    allow_pattern: Optional[List[str]] = None,
    ignore_pattern: Optional[List[str]] = None,
    credentials: Optional[AzureCredentials] = None
) -> None:
    """
    Download files from Azure Blob Storage to local directory.
    
    Args:
        model_path: Azure blob path in format "az://container/prefix"
        dst: Local destination directory
        allow_pattern: Optional list of glob patterns to include
        ignore_pattern: Optional list of glob patterns to exclude
        credentials: Optional AzureCredentials object
    """
    client = _create_client(credentials)

    if not model_path.endswith("/"):
        model_path = model_path + "/"

    container_name, base_dir, files = list_files(
        client, model_path, allow_pattern, ignore_pattern
    )
    
    if len(files) == 0:
        return

    container_client = client.get_container_client(container_name)

    for file in files:
        destination_file = os.path.join(
            dst,
            removeprefix(file, base_dir).lstrip("/")
        )
        local_dir = Path(destination_file).parent
        os.makedirs(local_dir, exist_ok=True)
        
        blob_client = container_client.get_blob_client(file)
        with open(destination_file, "wb") as download_file:
            download_file.write(blob_client.download_blob().readall())


def list_files(
    client: BlobServiceClient,
    path: str,
    allow_pattern: Optional[List[str]] = None,
    ignore_pattern: Optional[List[str]] = None
) -> Tuple[str, str, List[str]]:
    """
    List files in Azure Blob Storage at the given path.
    
    Args:
        client: BlobServiceClient instance
        path: Azure blob path
        allow_pattern: Optional list of glob patterns to include
        ignore_pattern: Optional list of glob patterns to exclude

    Returns:
        Tuple of (container_name, prefix, list_of_blob_names)
    """
    # Parse az://container/prefix format
    path = removeprefix(path, 'az://')
    parts = path.split('/', 1)
    container_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    # Reconstruct the prefix
    prefix = prefix.rstrip('/')
    
    if prefix:
        # This ensures a trailing slash without double-slashing
        prefix = posixpath.join(prefix, '')

    container_client = client.get_container_client(container_name)
    
    # List all blobs with the given prefix
    blob_items = container_client.list_blobs(name_starts_with=prefix)
    
    # Manually filter to achieve non-recursive behavior (like S3/GCS with delimiter)
    # Only include blobs that are directly in the prefix directory, not in subdirectories
    paths = []
    for item in blob_items:
        if hasattr(item, 'name'):
            # Remove the prefix to get the relative path
            relative_path = item.name[len(prefix):] if item.name.startswith(prefix) else item.name
            # If there's no '/' in the relative path, it's directly in this directory
            # If there is a '/', it's in a subdirectory and should be excluded
            if '/' not in relative_path:
                paths.append(item.name)

    # Filter out directories (blobs ending with /)
    paths = _filter_ignore(paths, ["*/"])
    
    if allow_pattern is not None:
        paths = _filter_allow(paths, allow_pattern)

    if ignore_pattern is not None:
        paths = _filter_ignore(paths, ignore_pattern)

    return container_name, prefix, paths


def _filter_allow(paths: List[str], patterns: List[str]) -> List[str]:
    return [
        path for path in paths if any(
            fnmatch.fnmatch(path, pattern) for pattern in patterns
        )
    ]


def _filter_ignore(paths: List[str], patterns: List[str]) -> List[str]:
    return [
        path for path in paths
        if not any(fnmatch.fnmatch(path, pattern) for pattern in patterns)
    ]


def removeprefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    return s
