from typing import Optional, List, Tuple
import fnmatch
import os
import boto3
from pathlib import Path

def glob(path: str, allow_pattern: Optional[List[str]] = None) -> List[str]:
    """
    List full file names from S3 path and filter by allow pattern.

    Args:
        s3: S3 client to use.
        path: The S3 path to list from.
        allow_pattern: A list of patterns of which files to pull.

    Returns:
        list[str]: List of full S3 paths allowed by the pattern
    """
    s3 = boto3.client("s3")
    if not path.endswith("/"):
        path = path + "/"
    bucket_name, _, paths = list_files(s3,
                                       path=path,
                                       allow_pattern=allow_pattern)
    return [f"s3://{bucket_name}/{path}" for path in paths]

def list_files(
        s3,
        path: str,
        allow_pattern: Optional[List[str]] = None,
        ignore_pattern: Optional[List[str]] = None
) -> Tuple[str, str, List[str]]:
    """
    List files from S3 path and filter by pattern.

    Args:
        s3: S3 client to use.
        path: The S3 path to list from.
        allow_pattern: A list of patterns of which files to pull.
        ignore_pattern: A list of patterns of which files not to pull.

    Returns:
        tuple[str, str, list[str]]: A tuple where:
            - The first element is the bucket name
            - The second element is string represent the bucket 
              and the prefix as a dir like string
            - The third element is a list of files allowed or 
              disallowed by pattern
    """
    parts = removeprefix(path, 's3://').split('/')
    prefix = '/'.join(parts[1:])
    bucket_name = parts[0]

    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    paths = [obj['Key'] for obj in objects.get('Contents', [])]

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

def pull_files(model_path: str,
                dst: str,
                allow_pattern: Optional[List[str]] = None,
                ignore_pattern: Optional[List[str]] = None,) -> None:
    if not model_path.endswith("/"):
        model_path = model_path + "/"

    s3 = boto3.client("s3")

    bucket_name, base_dir, files = list_files(s3, model_path,
                                                allow_pattern,
                                                ignore_pattern)
    if len(files) == 0:
        return

    for file in files:
        destination_file = os.path.join(
            dst,
            file.removeprefix(base_dir).lstrip("/"))
        local_dir = Path(destination_file).parent
        os.makedirs(local_dir, exist_ok=True)
        s3.download_file(bucket_name, file, destination_file)

def removeprefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    return s