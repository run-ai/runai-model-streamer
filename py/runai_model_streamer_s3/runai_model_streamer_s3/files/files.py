from typing import Optional, List, Tuple
from runai_model_streamer_s3.credentials.credentials import get_credentials, S3Credentials
import fnmatch
import os
import boto3
from pathlib import Path

def glob(path: str, allow_pattern: Optional[List[str]] = None, credentials: Optional[S3Credentials] = None) -> List[str]:
    session, _ = get_credentials(credentials)
    if session is None:
        s3 = boto3.client("s3")
    else:
        s3 = session.client("s3")

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
    if session is None:
        s3 = boto3.client("s3")
    else:
        s3 = session.client("s3")

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

def removeprefix(s: str, prefix: str) -> str:
    if s.startswith(prefix):
        return s[len(prefix):]
    return s