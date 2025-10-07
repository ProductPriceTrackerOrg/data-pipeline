#!/usr/bin/env python3
"""
Test script for uploading scraped data to Azure Data Lake Storage.
This is a standalone script that can be used to test the ADLS upload functionality.

Usage:
    python test_adls_upload.py
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Load environment variables
from dotenv import load_dotenv

# Find and load the root .env file (three levels up from test dir)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
dotenv_path = os.path.join(project_root, ".env")
print(f"Loading environment variables from: {dotenv_path}")
load_dotenv(dotenv_path)

# Azure Storage imports
from azure.storage.blob import BlobServiceClient, ContentSettings

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_to_adls(json_data: str, source_website: str, file_name: str = "data.json"):
    """
    Uploads a JSON string to ADLS as a JSON file with chunk-wise upload for large files.
    The final file will always be a single file named data.json (or the specified file_name),
    even when the upload happens internally in chunks.
    
    Args:
        json_data: Ready-to-upload JSON string with properly serialized data
        source_website: Name of the source website (used for partitioning)
        file_name: Name of the file to upload (default: data.json)
    """
    # --- 1. Get Azure Connection String from Environment Variable ---
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("Azure connection string not found in environment variables.")

    # --- 2. Define the partitioned path ---
    scrape_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    file_path = f"source_website={source_website}/scrape_date={scrape_date}/{file_name}"
    container_name = "raw-data"

    print(f"Will upload to: {container_name}/{file_path}")

    try:
        # --- 3. Connect to Azure and Upload ---
        print("Connecting to Azure Blob Storage...")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Check if container exists and create it if it doesn't
        container_exists = False
        print("Checking if container exists...")
        containers = blob_service_client.list_containers()
        for container in containers:
            if container.name == container_name:
                container_exists = True
                break
                
        if not container_exists:
            print(f"Container '{container_name}' does not exist. Creating it...")
            blob_service_client.create_container(name=container_name)
            print(f"Container '{container_name}' created successfully!")
            
        # Calculate file size
        data_size_mb = len(json_data.encode('utf-8')) / (1024 * 1024)  # Convert bytes to MB
        print(f"Data size: {data_size_mb:.2f} MB")
        
        # For large files, we'll use a staged upload approach
        if data_size_mb > 5:  # If file is larger than 5MB
            print(f"File size is large ({data_size_mb:.2f}MB), using staged upload approach")
            
            # Create a temporary file
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp_file:
                temp_path = temp_file.name
                # Write the JSON data to the temporary file
                temp_file.write(json_data.encode('utf-8'))
            
            try:
                # Get the container client
                container_client = blob_service_client.get_container_client(container_name)
                
                # Set up content settings
                from azure.storage.blob import ContentSettings
                content_settings = ContentSettings(content_type='application/json')
                
                # Create a simple progress indicator
                start_time = datetime.now()
                print("Upload started at:", start_time.strftime("%H:%M:%S"))
                print("Uploading in chunks...", flush=True)
                
                # Upload from file using staged upload
                with open(temp_path, 'rb') as data:
                    container_client.upload_blob(
                        name=file_path,
                        data=data,
                        overwrite=True,
                        content_settings=content_settings,
                        max_concurrency=4,
                        timeout=1200  # 20 minutes timeout
                    )
                
                # Show upload duration
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                print(f"Upload completed in {duration:.1f} seconds")
                
            finally:
                # Clean up the temporary file
                try:
                    os.unlink(temp_path)
                    print("Temporary file removed")
                except Exception as e:
                    print(f"Warning: Could not remove temporary file: {e}")
        else:
            # For smaller files, use standard upload
            print(f"File size is small ({data_size_mb:.2f}MB), using standard upload")
            
            # Get the blob client
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
            
            # Upload the data directly
            start_time = datetime.now()
            print("Upload started at:", start_time.strftime("%H:%M:%S"))
            blob_client.upload_blob(
                json_data,
                overwrite=True,
                content_settings=ContentSettings(content_type='application/json')
            )
            
            # Show upload duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"Upload completed in {duration:.1f} seconds")

        print(f"Upload to ADLS successful for {file_name}!")
        return True

    except Exception as e:
        print(f"ADLS upload error: {e}")
        logger.error(f"ADLS upload error: {e}", exc_info=True)
        return False

def main():
    """Main entry point for testing ADLS upload"""
    print("="*60)
    print(" ADLS UPLOAD TEST - CYBERDEALS")
    print("="*60)
    
        # Path to the scraped data file
    script_dir = os.path.dirname(os.path.abspath(__file__))
    scraped_data_dir = os.path.join(script_dir, '..', 'scraped_data')
    
    # List all JSON files in the scraped_data directory
    json_files = [f for f in os.listdir(scraped_data_dir) if f.endswith('.json')]
    
    if 'cyberdeals_lk_scrape_optimized.json' in json_files:
        scraped_file = os.path.join(scraped_data_dir, 'cyberdeals_lk_scrape_optimized.json')
    elif json_files:
        # If the specific file isn't found, use the first JSON file
        scraped_file = os.path.join(scraped_data_dir, json_files[0])
        print(f"Note: Using available JSON file: {json_files[0]}")
    else:
        scraped_file = None
    
    if not scraped_file or not os.path.exists(scraped_file):
        print(f"Error: Scraped data file not found at {scraped_file}")
        print("Please make sure the file exists before running this test.")
        return 1
    
    print(f"Found scraped data file: {scraped_file}")
    print(f"File size: {os.path.getsize(scraped_file) / (1024 * 1024):.2f} MB")
    
    # Load the JSON data
    try:
        with open(scraped_file, 'r', encoding='utf-8') as f:
            json_data = f.read()
        
        # Upload to ADLS
        success = upload_to_adls(
            json_data=json_data,
            source_website="cyberdeals",
            file_name="test_upload.json"  # Using a different name for test uploads
        )
        
        if success:
            print("✅ Test upload completed successfully!")
            return 0
        else:
            print("❌ Test upload failed!")
            return 1
            
    except Exception as e:
        print(f"Error during test: {e}")
        logger.error(f"Test error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())