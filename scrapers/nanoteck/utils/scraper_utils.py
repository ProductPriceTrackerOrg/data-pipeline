"""
HTTP utilities and helper functions for Nanotek web scraping
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Optional, Dict, Any, List
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, unquote

# Load environment variables
load_dotenv()

# Valid image extensions
VALID_IMAGE_EXTENSIONS = {".webp", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg"}
VALID_IMAGE_TYPES = {"webp", "jpg", "jpeg", "png", "gif", "bmp", "svg"}

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


def validate_image_url(url: str) -> bool:
    """
    Validate if a URL is a valid image URL with correct extension

    Args:
        url: Image URL to validate

    Returns:
        True if valid image URL, False otherwise
    """
    if not url or not isinstance(url, str):
        return False

    # Remove whitespace and convert to lowercase for checking
    url = url.strip()

    if not url:
        return False

    # Check if URL is valid (starts with http/https or is relative)
    if not (
        url.startswith("http://") or url.startswith("https://") or url.startswith("/")
    ):
        return False

    try:
        # Parse URL and decode any URL encoding
        parsed = urlparse(url)
        path = unquote(parsed.path.lower())

        # Remove query parameters and fragments for extension check
        path_without_query = path.split("?")[0].split("#")[0]

        # Check if path ends with valid image extension
        for ext in VALID_IMAGE_EXTENSIONS:
            if path_without_query.endswith(ext):
                return True

        # Check if extension is in query parameters (some CDNs use this format)
        query = parsed.query.lower()
        if query:
            for img_type in VALID_IMAGE_TYPES:
                if img_type in query:
                    if any(
                        param in query
                        for param in [
                            "format=",
                            "type=",
                            "ext=",
                            ".jpg",
                            ".png",
                            ".webp",
                            ".jpeg",
                        ]
                    ):
                        return True

        return False

    except Exception as e:
        logging.warning(f"Error validating image URL {url}: {e}")
        return False


def validate_and_filter_image_urls(urls: List[str]) -> List[str]:
    """
    Validate and filter a list of image URLs, keeping only valid ones

    Args:
        urls: List of image URLs to validate

    Returns:
        List of valid image URLs
    """
    if not urls:
        return []

    valid_urls = []
    for url in urls:
        if validate_image_url(url):
            valid_urls.append(url.strip())
        else:
            logging.debug(f"Filtered out invalid image URL: {url}")

    return valid_urls


def get_image_extension(url: str) -> Optional[str]:
    """
    Extract image extension from URL

    Args:
        url: Image URL

    Returns:
        Image extension (e.g., 'jpg', 'png') or None if not found
    """
    if not validate_image_url(url):
        return None

    try:
        parsed = urlparse(url)
        path = unquote(parsed.path.lower())
        path_without_query = path.split("?")[0].split("#")[0]

        # Extract extension
        for ext in VALID_IMAGE_EXTENSIONS:
            if path_without_query.endswith(ext):
                return ext.lstrip(".")

        # Check query parameters
        query = parsed.query.lower()
        if query:
            for img_type in VALID_IMAGE_TYPES:
                if f"format={img_type}" in query or f"type={img_type}" in query:
                    return img_type

        return None

    except Exception:
        return None


def clean_image_url(url: str) -> str:
    """
    Clean and normalize image URL

    Args:
        url: Raw image URL

    Returns:
        Cleaned image URL
    """
    if not url:
        return ""

    # Remove leading/trailing whitespace
    url = url.strip()

    # Remove image size parameters commonly used in CDNs
    url = re.sub(r"-\d+x\d+\.(webp|jpg|jpeg|png)", r".\1", url)

    # Ensure proper protocol
    if url.startswith("//"):
        url = "https:" + url

    return url


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
