"""
Utility functions for LifeMobile scraper
"""
import logging
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import urllib.parse


def setup_logging(log_file: str = "lifemobile_scraper.log", level: int = logging.INFO):
    """Setup logging configuration"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def clean_text(text: str) -> str:
    """Clean and normalize text data"""
    if not text:
        return ""
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text.strip())
    
    # Remove control characters
    text = ''.join(char for char in text if ord(char) >= 32 or char == '\n')
    
    return text


def clean_price(price: str) -> str:
    """Clean price string"""
    if not price:
        return ""
    
    # Remove currency symbols and extra text, keep numbers and decimal points
    price = re.sub(r'[^\d.,]', '', price)
    
    # Handle comma as thousand separator
    if ',' in price and '.' in price:
        # Assume comma is thousand separator if it comes before decimal
        parts = price.split('.')
        if len(parts[-1]) <= 2:  # Last part should be cents
            price = price.replace(',', '')
    elif ',' in price:
        # Check if comma is decimal separator
        comma_pos = price.rfind(',')
        if len(price) - comma_pos <= 3:  # Likely decimal separator
            price = price.replace(',', '.')
        else:  # Likely thousand separator
            price = price.replace(',', '')
    
    return price


def normalize_url(url: str, base_url: str = "") -> str:
    """Normalize URL for consistency"""
    if not url:
        return ""
    
    # Handle relative URLs
    if base_url and not url.startswith(('http://', 'https://')):
        url = urllib.parse.urljoin(base_url, url)
    
    # Parse and reconstruct URL to normalize
    parsed = urllib.parse.urlparse(url)
    
    # Remove fragment
    normalized = urllib.parse.urlunparse(parsed._replace(fragment=''))
    
    return normalized


def extract_numeric_id(text: str) -> Optional[str]:
    """Extract numeric ID from text"""
    if not text:
        return None
    
    # Find numeric parts
    numbers = re.findall(r'\d+', text)
    
    if numbers:
        return numbers[-1]  # Return the last/longest number found
    
    return None


def validate_required_fields(data: Dict[str, Any], required_fields: List[str]) -> bool:
    """Validate that required fields are present and non-empty"""
    for field in required_fields:
        value = data.get(field)
        if not value or (isinstance(value, str) and not value.strip()):
            return False
    return True


def clean_json_data(data: Union[List, Dict]) -> Union[List, Dict]:
    """Clean JSON data recursively"""
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            cleaned_value = clean_json_data(value)
            if cleaned_value is not None:  # Keep falsy values but exclude None
                cleaned[key] = cleaned_value
        return cleaned
    
    elif isinstance(data, list):
        cleaned = []
        for item in data:
            cleaned_item = clean_json_data(item)
            if cleaned_item is not None:
                cleaned.append(cleaned_item)
        return cleaned
    
    elif isinstance(data, str):
        return clean_text(data) if data else data
    
    else:
        return data


def validate_product_data(product: Dict[str, Any]) -> bool:
    """Validate product data structure"""
    required_fields = [
        'product_id_native',
        'product_url',
        'product_title',
        'variants'
    ]
    
    # Check required fields
    if not validate_required_fields(product, required_fields):
        return False
    
    # Check variants structure
    variants = product.get('variants', [])
    if not variants or not isinstance(variants, list):
        return False
    
    # Validate each variant
    for variant in variants:
        if not isinstance(variant, dict):
            return False
        
        variant_required = [
            'variant_id_native',
            'variant_title',
            'price_current',
            'availability_text'
        ]
        
        if not validate_required_fields(variant, variant_required):
            return False
    
    return True


def format_currency(amount: Union[str, float], currency: str = "Rs.") -> str:
    """Format currency amount"""
    if isinstance(amount, str):
        amount = clean_price(amount)
        if not amount:
            return ""
        try:
            amount = float(amount)
        except ValueError:
            return str(amount)
    
    if isinstance(amount, (int, float)):
        return f"{currency} {amount:,.2f}"
    
    return str(amount)


def calculate_discount_percentage(original: str, current: str) -> Optional[float]:
    """Calculate discount percentage"""
    try:
        original_price = float(clean_price(original))
        current_price = float(clean_price(current))
        
        if original_price > current_price > 0:
            discount = ((original_price - current_price) / original_price) * 100
            return round(discount, 2)
    except (ValueError, ZeroDivisionError):
        pass
    
    return None


def safe_filename(text: str, max_length: int = 100) -> str:
    """Create a safe filename from text"""
    if not text:
        return "unnamed"
    
    # Remove/replace unsafe characters
    safe = re.sub(r'[<>:"/\\|?*]', '_', text)
    safe = re.sub(r'[^\w\s\-_.]', '', safe)
    safe = re.sub(r'[-\s]+', '_', safe)
    
    return safe[:max_length].strip('_')


def get_file_size(file_path: str) -> Optional[int]:
    """Get file size in bytes"""
    try:
        return Path(file_path).stat().st_size
    except (OSError, FileNotFoundError):
        return None


def ensure_directory(path: str) -> bool:
    """Ensure directory exists"""
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        return True
    except OSError:
        return False


def timestamp_filename(base_name: str, extension: str = ".json") -> str:
    """Add timestamp to filename"""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    name_part = Path(base_name).stem
    return f"{name_part}_{timestamp}{extension}"


def load_json_file(file_path: str) -> Optional[Union[Dict, List]]:
    """Load JSON file with error handling"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.error(f"Error loading JSON file {file_path}: {e}")
        return None


def save_json_file(data: Union[Dict, List], file_path: str, indent: int = 2) -> bool:
    """Save data to JSON file"""
    try:
        ensure_directory(str(Path(file_path).parent))
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False, default=str)
        
        return True
    except (IOError, TypeError) as e:
        logging.error(f"Error saving JSON file {file_path}: {e}")
        return False


def get_domain(url: str) -> str:
    """Extract domain from URL"""
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.netloc.lower()
    except:
        return ""


def is_valid_url(url: str) -> bool:
    """Check if URL is valid"""
    try:
        result = urllib.parse.urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False


class ScrapingStats:
    """Helper class for tracking scraping statistics"""
    
    def __init__(self):
        self.start_time = datetime.now(timezone.utc)
        self.end_time = None
        self.products_scraped = 0
        self.errors_encountered = 0
        self.categories_found = 0
        
    def finish(self):
        """Mark scraping as finished"""
        self.end_time = datetime.now(timezone.utc)
    
    @property
    def duration(self) -> Optional[float]:
        """Get scraping duration in seconds"""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def rate(self) -> Optional[float]:
        """Get scraping rate (products per second)"""
        duration = self.duration
        if duration and duration > 0:
            return self.products_scraped / duration
        return None
    
    def __str__(self) -> str:
        return (
            f"ScrapingStats(products={self.products_scraped}, "
            f"errors={self.errors_encountered}, "
            f"duration={self.duration:.2f}s, "
            f"rate={self.rate:.2f}/s)"
        )