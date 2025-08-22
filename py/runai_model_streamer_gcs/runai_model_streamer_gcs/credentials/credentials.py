from typing import Tuple, Dict, Optional
from enum import Enum, auto

import google.auth
import google.auth.credentials

import os
import boto3

GCS_CREDENTIAL_TYPE = "GCS_CREDENTIAL_TYPE"
GCS_SA_KEY_PATH = "GCS_SA_KEY_PATH"
# TODO: Pass credentials through to the C++ API library.
RUNAI_STREAMER_GCS_CREDENTIAL_FILE = "RUNAI_STREAMER_GCS_CREDENTIAL_FILE"
# Testing Only: This variable is used by google-cloud-cpp for emulator testing
#   We use it here to allow Anonymous credential override.
CLOUD_STORAGE_EMULATOR_ENDPOINT = "CLOUD_STORAGE_EMULATOR_ENDPOINT"

class CredentialType(Enum):
    # Credentials provided explicitly via a JSON file.
    SERVICE_ACCOUNT_JSON = auto()
    # Workload Identity or Application Default Credentials.
    DEFAULT_CREDENTIALS = auto()

class GCSCredentials:
    def __init__(
        self,
        sa_key_file: Optional[str] = None,
    ):
        if sa_key_file:
            self.sa_key_path = sa_key_file
            self.credential_type = CredentialType.SERVICE_ACCOUNT_JSON
        else:
            self.credential_type = CredentialType.DEFAULT_CREDENTIALS

    def serialized_credentials(self) -> Dict[str, str]:
        return {
            GCS_CREDENTIAL_TYPE: self.credential_type.value,
            GCS_SA_KEY_PATH: self.sa_key_file,
        }

    def gcp_credentials(self) -> google.auth.credentials.Credentials:
        credentials = None
        endpoint_override = os.getenv(CLOUD_STORAGE_EMULATOR_ENDPOINT, default=False)
        if endpoint_override:
            credentials = google.auth.credentials.AnonymousCredentials()
        elif self.credential_type == CredentialType.SERVICE_ACCOUNT_JSON:
            credentials, _ = google.auth.load_credentials_from_file(self.sa_key_path)
        else:
            credentials, _ = google.auth.default()

        return credentials

def get_credentials() -> GCSCredentials:
    """
    Creates a GCS ServiceAccount credential if the environment variable RUNAI_STREAMER_GCS_CREDENTIAL_FILE is set.
    If the variable is not set, returns None and the original credentials.

    Returns:
        - GCSCredentials object with the resolved credentials (or original if session not created)
    """

    sa_key_file = os.getenv(RUNAI_STREAMER_GCS_CREDENTIAL_FILE, default=None)
    return GCSCredentials(sa_key_file)
