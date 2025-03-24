from typing import Optional
import re

GCS_PROTOCOL_PREFIX = "gs://"
S3_PROTOCOL_PREFIX = "s3://"
DEFAULT_GCS_ENDPOINT_URL = "https://storage.googleapis.com"

class S3Credentials:
    def __init__(
        self,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        session_token: Optional[str] = None,
        region_name: Optional[str] = None,
        endpoint: Optional[str] = None
    ):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.region_name = region_name
        self.endpoint = endpoint

def is_s3_path(path: str) -> bool:
    """
    Checks if the given string is an S3 path.

    :param path: The string to check.
    :return: True if it's an S3 path, False otherwise.
    """
    return path.startswith(S3_PROTOCOL_PREFIX)

def is_gs_path(path: str) -> bool:
    """
    Checks if the given string is an S3 path.

    :param path: The string to check.
    :return: True if it's an GCS path, False otherwise.
    """
    return path.startswith(GCS_PROTOCOL_PREFIX)

def gs_credentials(credentials : S3Credentials) -> S3Credentials:
    """
    Sets default GCS url endpoint
    Does not override if endpoint is already defined

    :param credentials: Original credentials
    :return: Modified credentials
    """
    if credentials is None:
        return S3Credentials(endpoint = DEFAULT_GCS_ENDPOINT_URL)
    
    if credentials.endpoint is None:
        credentials.endpoint = DEFAULT_GCS_ENDPOINT_URL
    
    return credentials

def convert_gs_path(path : str) -> str:
    """
    Replace GCS prefix with the unified object store prefix which is used by the libstreamer

    :param credentials: Original path
    :return: Modified path
    """
    stripped_path = path.removeprefix(GCS_PROTOCOL_PREFIX)
    converted_path = f"{S3_PROTOCOL_PREFIX}{stripped_path}"
    return converted_path
