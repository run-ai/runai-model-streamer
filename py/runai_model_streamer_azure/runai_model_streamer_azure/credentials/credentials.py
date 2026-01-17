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
    """
    
    def __init__(
        self,
        account_name: Optional[str] = None,
        endpoint: Optional[str] = None
    ):
        self.account_name = account_name
        self.endpoint = endpoint


def get_credentials(credentials: Optional[AzureCredentials] = None) -> AzureCredentials:
    """
    Resolves Azure credentials from various sources.
    
    Args:
        credentials: Optional AzureCredentials object with explicit credentials
        
    Returns:
        AzureCredentials object with resolved credentials
    """
    
    if credentials is None:
        credentials = AzureCredentials()
    
    # Check environment variables if not provided
    if not credentials.account_name:
        credentials.account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    
    if not credentials.endpoint:
        credentials.endpoint = os.environ.get("AZURE_STORAGE_ENDPOINT")
    
    return credentials


def create_blob_service_client(credentials: Optional[AzureCredentials] = None) -> BlobServiceClient:
    """
    Creates an Azure BlobServiceClient using DefaultAzureCredential.
    
    For local testing with Azurite, set AZURE_STORAGE_ACCOUNT_KEY environment variable
    to use account key authentication instead (token credentials only work with HTTPS).
    
    Args:
        credentials: Optional AzureCredentials object
        
    Returns:
        BlobServiceClient instance
        
    Raises:
        ValueError: If account name is not provided
    """
    
    creds = get_credentials(credentials)
    
    if not creds.account_name:
        raise ValueError(
            "Azure account name is required. Set AZURE_STORAGE_ACCOUNT_NAME environment variable "
            "or provide account_name in AzureCredentials."
        )
    
    account_url = creds.endpoint or f"https://{creds.account_name}.blob.core.windows.net"
    
    # Only use account key for HTTP endpoints (Azurite/local testing)
    # DefaultAzureCredential requires HTTPS, so this is inherently safe:
    # - Production Azure always uses HTTPS -> DefaultAzureCredential
    # - Azurite uses HTTP -> account key (if set)
    is_http_endpoint = account_url.startswith("http://")
    account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY") if is_http_endpoint else None
    
    if account_key:
        return BlobServiceClient(account_url=account_url, credential=account_key)
    
    # Use DefaultAzureCredential for production (HTTPS endpoints)
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)
