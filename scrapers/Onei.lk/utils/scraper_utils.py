"""
Utility functions for Onei.lk scraper
"""

import logging
import os
import re
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse, unquote

# Valid image extensions
VALID_IMAGE_EXTENSIONS = {".webp", ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg"}
VALID_IMAGE_TYPES = {"webp", "jpg", "jpeg", "png", "gif", "bmp", "svg"}


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
        # e.g., /image?format=webp or /image.php?type=jpg
        query = parsed.query.lower()
        if query:
            for img_type in VALID_IMAGE_TYPES:
                if img_type in query:
                    # Additional validation to ensure it's actually an image parameter
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
    # e.g., -150x150.jpg -> .jpg
    url = re.sub(r"-\d+x\d+\.(webp|jpg|jpeg|png)", r".\1", url)

    # Ensure proper protocol
    if url.startswith("//"):
        url = "https:" + url

    return url


def get_logger(name):
    """
    Get a logger instance with proper formatting

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(name)


def ensure_directory_exists(directory_path):
    """
    Ensure a directory exists, create if it doesn't

    Args:
        directory_path: Path to directory
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def format_timestamp():
    """
    Get formatted timestamp for file naming

    Returns:
        Formatted timestamp string
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")
