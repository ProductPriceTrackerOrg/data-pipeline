#!/usr/bin/env python3
"""
Test script for uploading scraped data to Azure Data Lake Storage using block blobs.
This approach explicitly breaks the file into blocks and uploads each block separately,
which is more resilient to network issues.

Usage:
    python test_block_upload.py
"""

import os
import sys
import uuid
import base64
import time
import logging
from datetime import datetime, timezone

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Load environment variables
from dotenv import load_dotenv

# Find and load the root .env file
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

def upload_large_file(connection_string, container_name, blob_path, file_path, block_size_mb=1):
    """
    Upload a large file to Azure Blob Storage using block blobs.
    
    Args:
        connection_string: Azure Storage connection string
        container_name: Container name
        blob_path: Path to the blob in the container
        file_path: Local file path to upload
        block_size_mb: Size of each block in MB
    """
    print(f"Starting upload of {file_path} to {container_name}/{blob_path}")
    print(f"Using block size: {block_size_mb}MB")
    
    # Create the blob service client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    
    # Check if container exists
    container_exists = False
    print("Checking if container exists...")
    try:
        containers = blob_service_client.list_containers()
        for container in containers:
            if container.name == container_name:
                container_exists = True
                break
    except Exception as e:
        print(f"Error checking containers: {e}")
        return False
                
    if not container_exists:
        print(f"Container '{container_name}' does not exist. Creating it...")
        try:
            blob_service_client.create_container(name=container_name)
            print(f"Container '{container_name}' created successfully!")
        except Exception as e:
            print(f"Error creating container: {e}")
            return False
    
    # Get the blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
    
    # Set content settings
    content_settings = ContentSettings(content_type='application/json')
    
    # Calculate file size
    file_size = os.path.getsize(file_path)
    file_size_mb = file_size / (1024 * 1024)
    print(f"File size: {file_size_mb:.2f}MB")
    
    # Calculate block size in bytes
    block_size = block_size_mb * 1024 * 1024
    
    # Calculate number of blocks
    num_blocks = (file_size + block_size - 1) // block_size  # Ceiling division
    
    print(f"Will upload in {num_blocks} blocks")
    
    # Generate block IDs
    block_ids = [base64.b64encode(f"block-{i}".encode()).decode() for i in range(num_blocks)]
    
    # Track which blocks were successfully uploaded
    blocks_uploaded = [False] * num_blocks
    
    # Start time for tracking total upload time
    start_time = datetime.now()
    print(f"Upload started at: {start_time.strftime('%H:%M:%S')}")
    
    try:
        with open(file_path, 'rb') as file:
            # Upload each block with retries
            for i in range(num_blocks):
                if blocks_uploaded[i]:
                    continue  # Skip already uploaded blocks
                
                # Read the block data
                file.seek(i * block_size)
                data = file.read(block_size)
                
                # Upload the block with retries
                max_retries = 5
                for attempt in range(max_retries):
                    try:
                        print(f"Uploading block {i+1}/{num_blocks} (Attempt {attempt+1}/{max_retries})...")
                        blob_client.stage_block(block_id=block_ids[i], data=data, timeout=120)
                        blocks_uploaded[i] = True
                        print(f"Block {i+1}/{num_blocks} uploaded successfully")
                        break
                    except Exception as e:
                        print(f"Error uploading block {i+1}: {e}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt  # Exponential backoff
                            print(f"Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)
                        else:
                            print(f"Failed to upload block {i+1} after {max_retries} attempts")
                            return False
            
            # Commit the blocks
            print("Committing all blocks...")
            try:
                blob_client.commit_block_list(block_ids, content_settings=content_settings)
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                print(f"Upload completed in {duration:.1f} seconds")
                print(f"Average upload speed: {file_size_mb / (duration/60):.2f}MB/minute")
                return True
            except Exception as e:
                print(f"Error committing blocks: {e}")
                return False
    
    except Exception as e:
        print(f"Error during upload: {e}")
        return False

def main():
    """Main entry point for testing ADLS upload"""
    print("="*60)
    print(" BLOCK BLOB UPLOAD TEST - CYBERDEALS")
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
        print(f"Error: Scraped data file not found")
        print("Please make sure the file exists before running this test.")
        return 1
    
    print(f"Found scraped data file: {scraped_file}")
    print(f"File size: {os.path.getsize(scraped_file) / (1024 * 1024):.2f} MB")
    
    # Azure storage details
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("Error: Azure connection string not found in environment variables.")
        return 1
    
    container_name = "raw-data"
    scrape_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    file_path = f"source_website=cyberdeals/scrape_date={scrape_date}/test_upload.json"
    
    print(f"Will upload to: {container_name}/{file_path}")
    
    # Upload the file using block blob upload
    success = upload_large_file(
        connection_string=connection_string,
        container_name=container_name,
        blob_path=file_path,
        file_path=scraped_file,
        block_size_mb=1  # Use 1MB blocks
    )
    
    if success:
        print("✅ Test upload completed successfully!")
        return 0
    else:
        print("❌ Test upload failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())