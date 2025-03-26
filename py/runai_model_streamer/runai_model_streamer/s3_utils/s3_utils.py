from typing import Optional
import re
import os
import importlib

GCS_PROTOCOL_PREFIX = "gs://"
S3_PROTOCOL_PREFIX = "s3://"
DEFAULT_GCS_ENDPOINT_URL = "https://storage.googleapis.com"
AWS_ENDPOINT_URL_ENV = "AWS_ENDPOINT_URL"
AWS_EC2_METADATA_DISABLED_ENV = "AWS_EC2_METADATA_DISABLED"
DEFAULT_AWS_EC2_METADATA_DISABLED = "true"

def get_s3_credentials_module():
    s3_module_name = "runai_model_streamer_s3"
    s3_credentials_module_name = "runai_model_streamer_s3.credentials.credentials"

    # Check if the main module exists first
    if importlib.util.find_spec(s3_module_name) is None:
        return None

    # Now check if the credentials module exists
    if importlib.util.find_spec(s3_credentials_module_name) is None:
        return None

    # Import and return the credentials module
    return importlib.import_module(s3_credentials_module_name)

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
    If the endpoint url is already gcs default endpoint, return true for setting the additional variables (most importantly the RUNAI_STREAMER_OVERRIDE_ENDPOINT_URL flag)

    :param path: The string to check.
    :return: True if it's an GCS path, False otherwise.
    """
    is_gcs_endpoint = os.environ.get(AWS_ENDPOINT_URL_ENV) == DEFAULT_GCS_ENDPOINT_URL
    return is_gcs_endpoint or path.startswith(GCS_PROTOCOL_PREFIX)

def set_gs_environment_variables() -> None:
    """
    Sets default GCS url endpoint
    Sets AWS_EC2_METADATA to speed authentication
    """

    os.environ.setdefault(AWS_ENDPOINT_URL_ENV, DEFAULT_GCS_ENDPOINT_URL)
    os.environ.setdefault(AWS_EC2_METADATA_DISABLED_ENV, DEFAULT_AWS_EC2_METADATA_DISABLED)


def convert_gs_path(path : str) -> str:
    """
    Replace GCS prefix with the unified object store prefix which is used by the libstreamer

    :param credentials: Original path
    :return: Modified path
    """
    if path.startswith(GCS_PROTOCOL_PREFIX):
        stripped_path = path.removeprefix(GCS_PROTOCOL_PREFIX)
        converted_path = f"{S3_PROTOCOL_PREFIX}{stripped_path}"
        return converted_path
    return path
