"""
Utility functions for Onei.lk scraper
"""
import logging
import os
from datetime import datetime

def get_logger(name):
    """
    Get a logger instance with proper formatting
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
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
    return datetime.now().strftime('%Y%m%d_%H%M%S')