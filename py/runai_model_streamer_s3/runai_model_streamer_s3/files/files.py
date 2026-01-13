from typing import Optional, List, Tuple
from runai_model_streamer_s3.credentials.credentials import get_credentials, S3Credentials
import fnmatch
import os
import boto3
from botocore.config import Config
from pathlib import Path
import posixpath

def glob(path: str, allow_pattern: Optional[List[str]] = None, credentials: Optional[S3Credentials] = None) -> List[str]:
    session, _ = get_credentials(credentials)
    use_virtual_addressing = os.getenv("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING", "1")
    
    client_config = None
    if use_virtual_addressing == "0":
        client_config = Config(s3={'addressing_style': 'path'})

    # Pass the config to the client constructor
    if session is None:
        s3 = boto3.client("s3", config=client_config)
    else:
        s3 = session.client("s3", config=client_config)
    
    if not path.endswith("/"):
        path = f"{path}/"
    bucket_name, _, keys = list_files(s3,
                                       path=path,
                                       allow_pattern=allow_pattern)
    return [f"s3://{bucket_name}/{key}" for key in keys]

def pull_files(model_path: str,
                dst: str,
                allow_pattern: Optional[List[str]] = None,
                ignore_pattern: Optional[List[str]] = None,
                credentials: Optional[S3Credentials] = None,) -> None:
    session, _ = get_credentials(credentials)
    use_virtual_addressing = os.getenv("RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING", "1")
    
    client_config = None
    if use_virtual_addressing == "0":
        client_config = Config(s3={'addressing_style': 'path'})

    # Pass the config to the client constructor
    if session is None:
        s3 = boto3.client("s3", config=client_config)
    else:
        s3 = session.client("s3", config=client_config)

    if not model_path.endswith("/"):
        model_path = model_path + "/"

    bucket_name, base_dir, files = list_files(s3, model_path,
                                                allow_pattern,
                                                ignore_pattern)
    if len(files) == 0:
        return

    for file in files:
        destination_file = os.path.join(
            dst,
            removeprefix(file, base_dir).lstrip("/"))
        local_dir = Path(destination_file).parent
        os.makedirs(local_dir, exist_ok=True)
        s3.download_file(bucket_name, file, destination_file)

def list_files(
        s3,
        path: str,
        allow_pattern: Optional[List[str]] = None,
        ignore_pattern: Optional[List[str]] = None
) -> Tuple[str, str, List[str]]:
    parts = removeprefix(path, 's3://').split('/')
    bucket_name = parts[0]
    
    # Reconstruct the prefix
    prefix = '/'.join(parts[1:]).rstrip('/')

    if prefix:
        # This ensures a trailing slash without double-slashing
        prefix = posixpath.join(prefix, '')

    paginator = s3.get_paginator('list_objects_v2')

    op_parameters = {
        'Bucket': bucket_name,
        'Prefix': prefix,
        'Delimiter': '/' # delimiter='/' so list is not recursive
    }

    paths = []
    for page in paginator.paginate(**op_parameters):
        # Contents is a list of files (no folders)
        if 'Contents' in page:
            for obj in page['Contents']:
                paths.append(obj['Key'])

    # Filter logic remains the same
    paths = _filter_ignore(paths, ["*/"])
    if allow_pattern is not None:
        paths = _filter_allow(paths, allow_pattern)

    if ignore_pattern is not None:
        paths = _filter_ignore(paths, ignore_pattern)

    return bucket_name, prefix, paths

def _filter_allow(paths: List[str], patterns: List[str]) -> List[str]:
    return [
        path for path in paths if any(
            fnmatch.fnmatch(path, pattern) for pattern in patterns)
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