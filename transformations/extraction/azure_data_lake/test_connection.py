#!/usr/bin/env python3
"""
Test Azure Data Lake Storage connection and discover data structure
"""
import os
import logging
from azure.storage.blob import BlobServiceClient
from config import AZURE_STORAGE_CONNECTION_STRING, AZURE_CONTAINER_NAME, validate_config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_adls_connection():
    """Test basic ADLS connectivity"""
    try:
        if not AZURE_STORAGE_CONNECTION_STRING:
            logger.error("AZURE_STORAGE_CONNECTION_STRING not found")
            return False
        
        # Create blob service client
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        
        # List containers
        containers = list(blob_service_client.list_containers())
        logger.info(f"✅ Connected to ADLS. Found {len(containers)} containers:")
        
        for container in containers:
            logger.info(f"   📁 {container.name}")
            
        return True
        
    except Exception as e:
        logger.error(f"ADLS connection failed: {str(e)}")
        return False

def discover_raw_data_structure():
    """Discover the structure of raw data in ADLS"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        
        logger.info(f"📂 Exploring {AZURE_CONTAINER_NAME} structure:")
        
        # Get unique folder paths
        sources = set()
        dates = set()
        file_count = 0
        
        for blob in container_client.list_blobs():
            file_count += 1
            logger.info(f"    {blob.name}")
            
            # Extract source and date info from path
            path_parts = blob.name.split('/')
            if 'source=' in blob.name:
                for part in path_parts:
                    if part.startswith('source='):
                        sources.add(part.replace('source=', ''))
            
            # Look for date patterns
            if any(part.startswith(('year=', 'day=', 'month=')) for part in path_parts):
                date_parts = [p for p in path_parts if p.startswith(('year=', 'month=', 'day='))]
                if len(date_parts) >= 3:  # year, month, day
                    dates.add('/'.join(date_parts))
            
            # Limit output for readability
            if file_count > 10:
                logger.info(f"   ... and {file_count - 10} more files")
                break
        
        logger.info(f" Discovered sources: {sorted(sources)}")
        logger.info(f" Sample date paths: {sorted(list(dates))[:5]}")
        
        return True
        
    except Exception as e:
        logger.error(f" Structure discovery failed: {str(e)}")
        return False

def download_sample_file():
    """Download a sample file to test the process"""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
        
        # Find first JSON file
        sample_blob = None
        for blob in container_client.list_blobs():
            if blob.name.endswith('.json'):
                sample_blob = blob.name
                break
        
        if not sample_blob:
            logger.warning(" No JSON files found in container")
            return False
        
        logger.info(f" Downloading sample file: {sample_blob}")
        
        # Download the file
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME,
            blob=sample_blob
        )
        
        blob_data = blob_client.download_blob().readall()
        
        # Create temp_data directory if it doesn't exist
        temp_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "temp_data")
        os.makedirs(temp_dir, exist_ok=True)
        
        # Save to temp_data
        output_file = os.path.join(temp_dir, "sample_download.json")
        with open(output_file, 'wb') as f:
            f.write(blob_data)
        
        logger.info(f" Successfully downloaded and saved to {output_file}")
        
        # Try to parse JSON and show basic info
        import json
        try:
            json_data = json.loads(blob_data.decode('utf-8'))
            if isinstance(json_data, list):
                logger.info(f" File contains {len(json_data)} products")
                if json_data:
                    logger.info(f" Sample product fields: {list(json_data[0].keys())}")
            else:
                logger.info(f" File contains single object with fields: {list(json_data.keys())}")
        except json.JSONDecodeError:
            logger.warning(" File is not valid JSON")
        
        return True
        
    except Exception as e:
        logger.error(f" Failed to download sample file: {str(e)}")
        return False

def main():
    """Run all connection tests"""
    logger.info(" Testing Azure Data Lake connection...\n")
    
    # Test 1: Configuration validation
    logger.info("Test 1: Configuration Validation")
    try:
        validate_config()
        logger.info(" Configuration validation passed\n")
    except Exception as e:
        logger.error(f" Configuration validation failed: {e}\n")
        return False
    
    # Test 2: Basic connection
    logger.info("Test 2: Azure Connection")
    if test_adls_connection():
        logger.info(" Connection test passed\n")
    else:
        logger.error(" Connection test failed - check your .env file\n")
        return False
    
    # Test 3: Structure discovery
    logger.info("Test 3: Data Structure Discovery")
    if discover_raw_data_structure():
        logger.info(" Structure discovery passed\n")
    else:
        logger.error(" Structure discovery failed\n")
    
    # Test 4: Sample file download
    logger.info("Test 4: Sample File Download")
    if download_sample_file():
        logger.info(" Download test passed\n")
    else:
        logger.error(" Download test failed\n")
    
    logger.info(" Connection testing complete!")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)