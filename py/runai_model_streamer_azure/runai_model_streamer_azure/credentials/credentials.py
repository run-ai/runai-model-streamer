from typing import Optional
import os

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobServiceClient
except ImportError:
    raise ImportError(
        "Azure Storage packages are not installed. "
        "Install them with: pip install azure-storage-blob azure-identity"
    )


class AzureCredentials:
    """
    Azure Blob Storage credentials configuration.

    Uses DefaultAzureCredential by default, which supports:
    - Managed Identity
    - Azure CLI
    - Environment credentials (AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_CLIENT_SECRET)
    - Visual Studio Code credentials

    For local testing, set AZURE_STORAGE_CONNECTION_STRING to use connection string auth.
    """

    def __init__(
        self,
        account_name: Optional[str] = None,
        endpoint: Optional[str] = None,
        connection_string: Optional[str] = None
    ):
        self.account_name = account_name
        self.endpoint = endpoint
        self.connection_string = connection_string


def get_credentials(credentials: Optional[AzureCredentials] = None) -> AzureCredentials:
    """
    Resolves Azure credentials from various sources.

    Priority order:
    1. Connection string (for local testing with Azurite)
    2. Account name + endpoint (for production with DefaultAzureCredential)

    Args:
        credentials: Optional AzureCredentials object with explicit credentials

    Returns:
        AzureCredentials object with resolved credentials
    """

    if credentials is None:
        credentials = AzureCredentials()

    # Check for connection string first (used for local testing)
    if not credentials.connection_string:
        credentials.connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

    # Check environment variables if not provided
    if not credentials.account_name:
        credentials.account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")

    if not credentials.endpoint:
        credentials.endpoint = os.environ.get("AZURE_STORAGE_ENDPOINT")

    return credentials


def create_blob_service_client(credentials: Optional[AzureCredentials] = None) -> BlobServiceClient:
    """
    Creates an Azure BlobServiceClient.

    Authentication priority:
    1. Connection string (AZURE_STORAGE_CONNECTION_STRING) - for local testing with Azurite
    2. DefaultAzureCredential with account URL - for production

    Args:
        credentials: Optional AzureCredentials object

    Returns:
        BlobServiceClient instance

    Raises:
        ValueError: If neither connection string nor account name/endpoint is provided
    """

    creds = get_credentials(credentials)

    # Use connection string if available (for Azurite/local testing)
    if creds.connection_string:
        return BlobServiceClient.from_connection_string(creds.connection_string)

    # Fall back to account name or endpoint + DefaultAzureCredential (for production)
    if not creds.account_name and not creds.endpoint:
        raise ValueError(
            "Azure credentials required. Set AZURE_STORAGE_CONNECTION_STRING for local testing, "
            "or AZURE_STORAGE_ACCOUNT_NAME/AZURE_STORAGE_ENDPOINT for production with DefaultAzureCredential."
        )

    account_url = creds.endpoint or f"https://{creds.account_name}.blob.core.windows.net"

    # Use DefaultAzureCredential for production (HTTPS endpoints)
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)
