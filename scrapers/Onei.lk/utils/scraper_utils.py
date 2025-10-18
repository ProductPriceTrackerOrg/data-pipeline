"""
Utility functions for Onei.lk scraper
"""

import logging
import os
from datetime import datetime, timezone


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
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def filter_valid_image_urls(image_urls):
    """
    Filter image URLs to only include valid webp, jpg, png, jpeg formats

    Args:
        image_urls: List of image URLs to filter

    Returns:
        List of valid image URLs
    """
    if not image_urls:
        return []

    valid_urls = []
    valid_extensions = (".webp", ".jpg", ".jpeg", ".png")

    for url in image_urls:
        if not url or url.strip() == "":
            continue

        # Skip SVG placeholders and invalid URLs
        if "data:image/svg+xml" in url:
            continue
        if "woocommerce-placeholder.png" in url:
            continue
        if url.startswith("data:"):
            continue

        # Check if URL ends with valid image extension
        url_lower = url.lower()
        if any(url_lower.endswith(ext) for ext in valid_extensions):
            valid_urls.append(url)
        # Also include URLs that contain valid extensions (for query params)
        elif any(ext in url_lower for ext in valid_extensions):
            valid_urls.append(url)
        # Check for onei.lk domain images without clear extension
        elif "onei.lk/wp-content/uploads/" in url and not url.endswith("/"):
            valid_urls.append(url)

    return valid_urls
