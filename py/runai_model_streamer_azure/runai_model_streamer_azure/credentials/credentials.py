from typing import Optional
import os

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class AzureCredentials:
    """
    Azure Blob Storage credentials configuration.

    Authentication methods (checked in this order):
    1. Connection string: Set AZURE_STORAGE_CONNECTION_STRING
    2. Storage account key: Set AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY
    3. DefaultAzureCredential: Set AZURE_STORAGE_ACCOUNT_NAME (uses Managed Identity, Azure CLI, etc.)

    If values are not provided explicitly, they are loaded from environment variables:
    - AZURE_STORAGE_CONNECTION_STRING
    - AZURE_STORAGE_ACCOUNT_NAME
    - AZURE_STORAGE_ACCOUNT_KEY
    """

    def __init__(
        self,
        account_name: Optional[str] = None,
        account_key: Optional[str] = None,
        connection_string: Optional[str] = None,
        credential: Optional[DefaultAzureCredential] = None
    ):
        self.connection_string = connection_string or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        self.account_name = account_name or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
        self.account_key = account_key or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
        if credential is None and not self.connection_string and not self.account_key:
            credential = DefaultAzureCredential()
        self.credential = credential
        self._validate()

    def _validate(self) -> None:
        """Validates that sufficient credentials are available to create a client."""
        if not self.connection_string and not self.account_name:
            raise ValueError(
                "Azure credentials required. Set AZURE_STORAGE_CONNECTION_STRING for local testing, "
                "or AZURE_STORAGE_ACCOUNT_NAME for production with DefaultAzureCredential."
            )


def get_credentials() -> AzureCredentials:
    """
    Creates Azure credentials from environment variables.

    Returns:
        AzureCredentials object with credentials loaded from environment
        
    Raises:
        ValueError: If neither connection string nor account name is available
    """
    return AzureCredentials()
