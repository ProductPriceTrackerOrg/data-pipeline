"""
HTTP utilities and helper functions for Nanotek web scraping
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Azure Storage imports with fallback
try:
    from azure.storage.blob import BlobServiceClient

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False


def ensure_output_directory(directory: str) -> None:
    """Ensure output directory exists"""
    if not os.path.exists(directory):
        os.makedirs(directory)


def save_json_data(data: Any, filepath: str, indent: int = 2) -> bool:
    """
    Save data to JSON file

    Args:
        data: Data to save
        filepath: Output file path
        indent: JSON indentation

    Returns:
        True if successful, False otherwise
    """
    try:
        ensure_output_directory(os.path.dirname(filepath))

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, default=str)

        logging.info(f"Data saved to: {filepath}")
        return True

    except Exception as e:
        logging.error(f"Error saving JSON to {filepath}: {e}")
        return False


def load_json_data(filepath: str) -> Optional[Dict[str, Any]]:
    """
    Load data from JSON file

    Args:
        filepath: File path to load

    Returns:
        Loaded data or None if failed
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.warning(f"File not found: {filepath}")
        return None
    except Exception as e:
        logging.error(f"Error loading JSON from {filepath}: {e}")
        return None


def setup_logging(log_file: str = "scraper.log", level: str = "INFO") -> None:
    """Setup logging configuration"""
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Create logs directory
    os.makedirs("logs", exist_ok=True)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # File handler
    file_handler = logging.FileHandler(os.path.join("logs", log_file), encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Configure root logger
    logging.basicConfig(
        level=log_level, handlers=[file_handler, console_handler], force=True
    )


def create_backup_filename(original_filename: str) -> str:
    """Create backup filename with timestamp"""
    name, ext = os.path.splitext(original_filename)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{name}_backup_{timestamp}{ext}"


def upload_to_adls(
    json_data: str, source_website: str, blob_name_suffix: str = ""
) -> bool:
    """
    Upload JSON data to Azure Data Lake Storage

    Args:
        json_data: JSON string to upload
        source_website: Source website name for partitioning
        blob_name_suffix: Additional suffix for blob name

    Returns:
        True if successful, False otherwise
    """
    if not AZURE_AVAILABLE:
        logging.warning(
            "Azure SDK not available. Install with: pip install azure-storage-blob"
        )
        return False

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        logging.error("Azure connection string not found in environment variables")
        return False

    # Define partitioned path following data lake best practices
    scrape_date = datetime.now().strftime("%Y-%m-%d")
    scrape_time = datetime.now().strftime("%H-%M-%S")

    if blob_name_suffix == "complete":
        # For complete dataset, use a more descriptive name
        file_name = f"nanotek_products_complete_{scrape_date}_{scrape_time}.json"
    elif blob_name_suffix:
        file_name = f"data_{blob_name_suffix}_{scrape_time}.json"
    else:
        file_name = f"data_{scrape_time}.json"

    file_path = f"source_website={source_website}/scrape_date={scrape_date}/{file_name}"
    container_name = "raw-data"

    try:
        # Connect to Azure and upload
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=file_path
        )

        logging.info(f"☁️ Uploading to Azure: {container_name}/{file_path}")

        # Create comprehensive metadata
        data_size_mb = round(len(json_data) / (1024 * 1024), 2)
        metadata = {
            "source_website": source_website,
            "scrape_date": scrape_date,
            "scrape_time": scrape_time,
            "data_size_mb": str(data_size_mb),
            "scraper_version": "2.0",
            "upload_type": blob_name_suffix or "data",
            "content_type": "application/json",
            "encoding": "utf-8",
        }

        # Add additional metadata for complete datasets
        if blob_name_suffix == "complete":
            try:
                # Parse JSON to get product count
                data = json.loads(json_data)
                if isinstance(data, dict) and "metadata" in data:
                    metadata.update(
                        {
                            "total_products": str(
                                data["metadata"].get("total_products", 0)
                            ),
                            "success_rate": str(
                                data["metadata"].get("success_rate_percent", 0)
                            ),
                            "scraping_duration": str(
                                data["metadata"].get("scraping_duration", "unknown")
                            ),
                        }
                    )
            except:
                pass

        # Upload with comprehensive metadata
        blob_client.upload_blob(json_data, overwrite=True, metadata=metadata)

        logging.info(f"✅ Successfully uploaded to Azure: {file_path}")
        return True

    except Exception as e:
        logging.error(f"❌ Azure upload failed: {e}")
        return False


def validate_azure_configuration() -> bool:
    """Validate Azure configuration and connectivity"""
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        logging.warning("Azure connection string not found in environment variables")
        return False

    try:
        # Test Azure connectivity
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        # Test connection by listing containers (lightweight operation)
        containers = []
        container_iter = blob_service_client.list_containers()
        count = 0
        for container in container_iter:
            containers.append(container)
            count += 1
            if count >= 1:  # Just get one container to test connectivity
                break
        logging.info("✅ Azure connectivity validated successfully")
        return True
    except Exception as e:
        logging.error(f"❌ Azure connectivity test failed: {e}")
        return False


def filter_image_urls(image_urls: list) -> list:
    """
    Filter image URLs to only include valid image formats

    Args:
        image_urls: List of image URLs

    Returns:
        List of filtered image URLs containing only .webp, .jpg, .jpeg, .png
    """
    valid_extensions = [".webp", ".jpg", ".jpeg", ".png"]
    filtered_urls = []

    for url in image_urls:
        if url and isinstance(url, str):
            # Check if URL ends with any valid extension (case insensitive)
            url_lower = url.lower()
            if any(url_lower.endswith(ext) for ext in valid_extensions):
                filtered_urls.append(url)

    return filtered_urls


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    Nanotek Web Scraper                      ║
║                  Enhanced Data Pipeline                      ║
║                    with Azure Integration                    ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
