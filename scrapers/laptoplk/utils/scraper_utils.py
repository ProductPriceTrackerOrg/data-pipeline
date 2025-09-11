"""
Utility functions for the Laptop.lk scraper
"""
import os
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

def setup_logging(config: Dict[str, Any] = None) -> None:
    """Setup logging configuration"""
    default_config = {
        "level": logging.INFO,
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "datefmt": "%Y-%m-%d %H:%M:%S",
    }
    
    config = config or default_config
    
    logging.basicConfig(
        level=config.get("level", logging.INFO),
        format=config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        datefmt=config.get("datefmt", "%Y-%m-%d %H:%M:%S"),
    )

def ensure_output_directory(directory_path: str) -> None:
    """Ensure the output directory exists"""
    os.makedirs(directory_path, exist_ok=True)

def save_json_data(data: List[Dict[str, Any]], filepath: str) -> bool:
    """Save data to a JSON file"""
    try:
        directory = os.path.dirname(filepath)
        ensure_output_directory(directory)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Data successfully saved to {filepath}")
        return True
    except Exception as e:
        logging.error(f"Error saving data to {filepath}: {e}")
        return False

def create_backup_filename(original_path: str) -> str:
    """Create a backup filename with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    directory = os.path.dirname(original_path)
    filename, ext = os.path.splitext(os.path.basename(original_path))
    return os.path.join(directory, f"{filename}_{timestamp}_backup{ext}")

def load_json_data(filepath: str) -> Optional[List[Dict[str, Any]]]:
    """Load data from a JSON file"""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        return None
    except Exception as e:
        logging.error(f"Error loading data from {filepath}: {e}")
        return None
