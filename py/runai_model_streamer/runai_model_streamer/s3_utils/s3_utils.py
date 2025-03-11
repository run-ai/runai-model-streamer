from typing import Optional
import re

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
    return bool(re.match(r"^s3://[^/]+/.+", path))
