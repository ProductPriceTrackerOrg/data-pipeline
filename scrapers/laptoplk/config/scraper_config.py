"""
Configuration settings for Laptop.lk scraper
"""
import os
import logging
from typing import Dict, List

# Base configuration
BASE_URL = "https://www.laptop.lk"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap_index.xml"

# Performance settings
MAX_CONCURRENT_REQUESTS = 25
REQUEST_DELAY = 0.1  # seconds between requests
TIMEOUT = 30  # request timeout in seconds
MAX_RETRIES = 3

# Request headers to mimic a real browser
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Output settings
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scraped_data")
OUTPUT_FILE = "laptop_lk_scrape.json"
BACKUP_OUTPUT = True

# Azure Storage settings
AZURE_CONTAINER_NAME = "raw-data"
AZURE_SOURCE_WEBSITE = "laptop.lk"

# Logging Configuration
LOGGING_CONFIG = {
    "level": logging.INFO,
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "datefmt": "%Y-%m-%d %H:%M:%S",
}
