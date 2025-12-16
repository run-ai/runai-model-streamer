from typing import Tuple, Dict, Optional

import os
import boto3

AWS_CA_BUNDLE_ENV = "AWS_CA_BUNDLE"

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

def get_credentials(credentials: Optional[S3Credentials] = None) -> Tuple[boto3.Session, S3Credentials]:
    """
    Creates a boto3 session only if the environment variable RUNAI_STREAMER_NO_BOTO3_SESSION is set.
    If the variable is not set, returns None and the original credentials.

    Returns:
        - boto3.Session object (or None if RUNAI_STREAMER_NO_BOTO3_SESSION is set)
        - S3Credentials object with the resolved credentials (or original if session not created)
    """

    if "RUNAI_STREAMER_NO_BOTO3_SESSION" in os.environ:
        return None, credentials if credentials else S3Credentials()

    session = boto3.Session(
        aws_access_key_id=credentials.access_key_id if credentials else None,
        aws_secret_access_key=credentials.secret_access_key if credentials else None,
        aws_session_token=credentials.session_token if credentials else None,
        region_name=credentials.region_name if credentials else None
    )

    # Retrieve the actual credentials (could be from env vars, IAM role, etc.)
    resolved_credentials = session.get_credentials()
    frozen_creds = resolved_credentials.get_frozen_credentials() if resolved_credentials else None

    # Create a new S3Credentials object with the resolved credentials
    new_credentials = S3Credentials(
        access_key_id=frozen_creds.access_key if frozen_creds else None,
        secret_access_key=frozen_creds.secret_key if frozen_creds else None,
        session_token=frozen_creds.token if frozen_creds else None,
        region_name=session.region_name,
        endpoint=credentials.endpoint if credentials else None, 
    )

    # set ca_bundle if exists and AWS_CA_BUNDLE is undefined
    if AWS_CA_BUNDLE_ENV not in os.environ:
        ca_bundle = session._session.get_config_variable("ca_bundle")
        if ca_bundle is not None:
            os.environ.setdefault(AWS_CA_BUNDLE_ENV, ca_bundle)
 
    return session, new_credentials