#!/usr/bin/env python3
"""
Configuration settings for Azure Data Lake extraction
"""
import os
from dotenv import load_dotenv

# Load environment variables from the project root .env file
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))), '.env')
load_dotenv(env_path)

# Azure Data Lake Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "raw-data")

# Data Lake folder structure configuration
RAW_DATA_PATH_PATTERN = "source_website={source}/scrape_date={date}/"

# Supported sources (will be auto-discovered, but these are expected)
EXPECTED_SOURCES = ["appleme", "simplytek", "onei.lk"]

# File patterns
JSON_FILE_EXTENSIONS = [".json"]

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Validation settings
MIN_PRODUCTS_PER_SOURCE = 10  # Minimum expected products per source
MAX_FILE_SIZE_MB = 100  # Maximum file size in MB

def validate_config():
    """Validate configuration settings"""
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment variables")
    
    return True

if __name__ == "__main__":
    print("🔧 Configuration Check:")
    print(f"Azure Connection String: {'Found' if AZURE_STORAGE_CONNECTION_STRING else 'Missing'}")
    print(f"Container Name: {AZURE_CONTAINER_NAME}")
    print(f"Expected Sources: {EXPECTED_SOURCES}")
    
    try:
        validate_config()
        print("Configuration is valid!")
    except Exception as e:
        print(f"Configuration error: {e}")