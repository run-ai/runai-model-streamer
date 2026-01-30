from typing import Optional
import os

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class AzureCredentials:
    """
    Azure Blob Storage credentials configuration.

    Uses DefaultAzureCredential by default, which supports:
    - Managed Identity
    - Azure CLI
    - Environment credentials (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
    - Visual Studio Code credentials

    For local testing, set AZURE_STORAGE_CONNECTION_STRING to use connection string auth.
    
    If values are not provided explicitly, they are loaded from environment variables:
    - AZURE_STORAGE_CONNECTION_STRING
    - AZURE_STORAGE_ACCOUNT_NAME
    - AZURE_STORAGE_ENDPOINT
    """

    def __init__(
        self,
        account_name: Optional[str] = None,
        endpoint: Optional[str] = None,
        connection_string: Optional[str] = None,
        credential: Optional[DefaultAzureCredential] = None
    ):
        self.connection_string = connection_string or os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        self.account_name = account_name or os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
        self.endpoint = endpoint or os.environ.get("AZURE_STORAGE_ENDPOINT")
        self._credential = credential

    @property
    def credential(self) -> Optional[DefaultAzureCredential]:
        """Returns the Azure credential object, creating one if not set and connection string is not used."""
        if self._credential is None and not self.connection_string:
            self._credential = DefaultAzureCredential()
        return self._credential
    
    def validate(self) -> None:
        """Validates that sufficient credentials are available to create a client."""
        if not self.connection_string and not self.account_name and not self.endpoint:
            raise ValueError(
                "Azure credentials required. Set AZURE_STORAGE_CONNECTION_STRING for local testing, "
                "or AZURE_STORAGE_ACCOUNT_NAME/AZURE_STORAGE_ENDPOINT for production with DefaultAzureCredential."
            )


def get_credentials(credentials: Optional[AzureCredentials] = None) -> AzureCredentials:
    """
    Resolves Azure credentials from various sources.

    Args:
        credentials: Optional AzureCredentials object with explicit credentials.
                     If None, creates one that loads from environment variables.

    Returns:
        AzureCredentials object with resolved credentials
        
    Raises:
        ValueError: If neither connection string nor account name/endpoint is available
    """
    if credentials is None:
        credentials = AzureCredentials()
    
    credentials.validate()
    return credentials
