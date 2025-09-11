"""
Azure Data Lake Storage utilities for Onei.lk scraper
"""
import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.core.exceptions import AzureError, ResourceNotFoundError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

class AzureDataLakeClient:
    """Client for Azure Data Lake Storage operations"""
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize Azure Data Lake client
        
        Args:
            connection_string: Azure storage connection string. If None, reads from environment.
        """
        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not self.connection_string:
            raise ValueError("Azure connection string not found. Check your .env file.")
        
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
            logger.info("Azure Data Lake client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Azure client: {e}")
            raise
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the Azure Data Lake connection
        
        Returns:
            Dict containing test results
        """
        test_results = {
            "connection_status": "failed",
            "account_name": None,
            "containers": [],
            "error": None
        }
        
        try:
            # Get account properties
            account_info = self.blob_service_client.get_account_information()
            test_results["account_name"] = account_info.get("account_kind", "Unknown")
            
            # List containers
            containers = self.blob_service_client.list_containers()
            test_results["containers"] = [container.name for container in containers]
            
            test_results["connection_status"] = "success"
            logger.info("Azure connection test successful")
            
        except Exception as e:
            test_results["error"] = str(e)
            logger.error(f"Azure connection test failed: {e}")
        
        return test_results
    
    def create_container_if_not_exists(self, container_name: str) -> bool:
        """
        Create container if it doesn't exist
        
        Args:
            container_name: Name of the container to create
            
        Returns:
            True if container exists or was created successfully
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            container_client.create_container()
            logger.info(f"Container '{container_name}' created successfully")
            return True
        except ResourceNotFoundError:
            logger.info(f"Container '{container_name}' already exists")
            return True
        except Exception as e:
            logger.error(f"Failed to create container '{container_name}': {e}")
            return False
    
    def upload_json_data(self, data: str, source_website: str, container_name: str = "raw-data") -> Dict[str, Any]:
        """
        Upload JSON data to Azure Data Lake Storage
        
        Args:
            data: JSON string to upload
            source_website: Source website name for partitioning
            container_name: Container name (default: "raw-data")
            
        Returns:
            Dict containing upload results
        """
        upload_results = {
            "success": False,
            "blob_url": None,
            "file_path": None,
            "error": None
        }
        
        try:
            # Create partitioned file path
            scrape_date = datetime.now().strftime('%Y-%m-%d')
            timestamp = datetime.now().strftime('%H-%M-%S')
            file_path = f"source_website={source_website}/scrape_date={scrape_date}/data_{timestamp}.json"
            
            # Ensure container exists
            self.create_container_if_not_exists(container_name)
            
            # Upload data
            blob_client = self.blob_service_client.get_blob_client(
                container=container_name, 
                blob=file_path
            )
            
            blob_client.upload_blob(data, overwrite=True)
            
            upload_results.update({
                "success": True,
                "blob_url": blob_client.url,
                "file_path": file_path
            })
            
            logger.info(f"Successfully uploaded data to {container_name}/{file_path}")
            
        except Exception as e:
            upload_results["error"] = str(e)
            logger.error(f"Failed to upload data: {e}")
        
        return upload_results
    
    def list_blobs(self, container_name: str, prefix: str = "") -> list:
        """
        List blobs in a container
        
        Args:
            container_name: Container name
            prefix: Blob name prefix filter
            
        Returns:
            List of blob names
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list blobs: {e}")
            return []