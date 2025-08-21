from typing import Optional, List, Tuple
from runai_model_streamer_gcs.credentials.credentials import get_credentials, GCSCredentials

import fnmatch
import os
from google.cloud import storage
from pathlib import Path

def _create_client() -> storage.client.Client:
    credentials = get_credentials()
    return storage.Client(credentials = credentials.gcp_credentials())

def glob(path: str, allow_pattern: Optional[List[str]] = None) -> List[str]:
    gcs = _create_client()

    if not path.endswith("/"):
        path = f"{path}/"
    bucket_name, _, keys = list_files(gcs,
                                      path=path,
                                      allow_pattern=allow_pattern)
    return [f"gs://{bucket_name}/{key}" for key in keys]

def pull_files(model_path: str,
                dst: str,
                allow_pattern: Optional[List[str]] = None,
                ignore_pattern: Optional[List[str]] = None) -> None:
    gcs = _create_client()

    if not model_path.endswith("/"):
        model_path = model_path + "/"

    bucket_name, base_dir, files = list_files(gcs, model_path,
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
        bucket = gcs.get_bucket(bucket_name)
        blob = bucket.blob(file)
        blob.download_to_filename(destination_file)

def list_files(
        gcs: storage.client.Client,
        path: str,
        allow_pattern: Optional[List[str]] = None,
        ignore_pattern: Optional[List[str]] = None
) -> Tuple[str, str, List[str]]:
    parts = removeprefix(path, 'gs://').split('/')
    prefix = '/'.join(parts[1:])
    bucket_name = parts[0]

    bucket = gcs.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    paths = [blob.name for blob in blobs]

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
