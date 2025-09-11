"""
Simple Azure Data Lake Storage connection test
"""
import os
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_azure_connection():
    """Test basic Azure connection"""
    print("=" * 60)
    print("TESTING AZURE DATA LAKE CONNECTION")
    print("=" * 60)
    
    try:
        # Get connection string from .env
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            print("❌ No Azure connection string found in .env file")
            return False
        
        print("✅ Connection string found")
        
        # Initialize client
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        print("✅ Azure client initialized successfully")
        
        # Test connection by listing containers
        containers = list(blob_service_client.list_containers())
        container_names = [container.name for container in containers]
        
        print(f"✅ Connection successful!")
        print(f"📁 Found {len(containers)} containers: {container_names}")
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_upload_sample_data():
    """Test uploading sample data"""
    print("\n" + "=" * 60)
    print("TESTING DATA UPLOAD")
    print("=" * 60)
    
    try:
        # Get connection string
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Create sample data
        sample_data = {
            "test_run": {
                "timestamp": datetime.now().isoformat(),
                "source": "onei.lk",
                "test_product": {
                    "product_id": "test_001",
                    "title": "Test Product",
                    "price": "1000.00"
                }
            }
        }
        
        # Convert to JSON
        json_data = json.dumps(sample_data, indent=2)
        
        # Create file path
        scrape_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = datetime.now().strftime('%H-%M-%S')
        file_path = f"source_website=onei.lk/scrape_date={scrape_date}/test_data_{timestamp}.json"
        container_name = "raw-data"
        
        # Upload
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=file_path
        )
        
        blob_client.upload_blob(json_data, overwrite=True)
        
        print(f"✅ Upload successful!")
        print(f"📁 Container: {container_name}")
        print(f"📄 File: {file_path}")
        print(f"🔗 URL: {blob_client.url}")
        
        return True
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

def main():
    """Run tests"""
    print("🚀 Starting Azure Data Lake Storage Tests")
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test 1: Connection
    connection_success = test_azure_connection()
    
    # Test 2: Upload (only if connection works)
    upload_success = False
    if connection_success:
        upload_success = test_upload_sample_data()
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    print(f"Connection Test: {'✅ PASSED' if connection_success else '❌ FAILED'}")
    print(f"Upload Test: {'✅ PASSED' if upload_success else '❌ FAILED'}")
    
    if connection_success and upload_success:
        print("\n🎉 All tests passed! Your Azure connection is working correctly.")
        print("You can now run your main scraper with confidence.")
    else:
        print("\n⚠️  Some tests failed. Please check:")
        print("1. Your .env file has the correct AZURE_STORAGE_CONNECTION_STRING")
        print("2. Your Azure credentials are valid")
        print("3. You have internet connection")

if __name__ == "__main__":
    main()