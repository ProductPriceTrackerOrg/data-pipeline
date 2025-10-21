"""
HTTP utilities and helper functions for web scraping
"""
import aiohttp
import asyncio
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import time
from urllib.parse import urljoin, urlparse
import os

from config.scraper_config import (
    DEFAULT_HEADERS, MAX_CONCURRENT_REQUESTS, REQUEST_DELAY, 
    TIMEOUT, MAX_RETRIES, RETRY_DELAY, BASE_URL
)


class ScrapingSession:
    """Async HTTP session manager for efficient scraping"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.logger = logging.getLogger(__name__)
        self.request_count = 0
        self.successful_requests = 0
        self.failed_requests = 0
        
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT_REQUESTS,
            limit_per_host=MAX_CONCURRENT_REQUESTS,
            ttl_dns_cache=300,
            use_dns_cache=True,
            force_close=False,
            enable_cleanup_closed=True,
        )
        
        timeout = aiohttp.ClientTimeout(total=TIMEOUT)
        
        # Ensure we set headers with proper compression support
        headers = DEFAULT_HEADERS.copy()
        if "Accept-Encoding" in headers:
            # Make sure it includes gzip even if brotli fails
            if "br" in headers["Accept-Encoding"] and "gzip" not in headers["Accept-Encoding"]:
                headers["Accept-Encoding"] = "gzip, deflate, br"
        
        self.session = aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            timeout=timeout
        )
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def fetch_json(self, url: str, retries: int = MAX_RETRIES) -> Optional[Dict[str, Any]]:
        """
        Fetch JSON data from URL with retry logic and rate limiting
        
        Args:
            url: URL to fetch
            retries: Number of retry attempts
            
        Returns:
            JSON data as dictionary or None if failed
        """
        async with self.semaphore:
            for attempt in range(retries + 1):
                try:
                    self.request_count += 1
                    self.logger.info(f"Fetching: {url} (attempt {attempt + 1})")
                    
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.successful_requests += 1
                            self.logger.debug(f"Successfully fetched: {url}")
                            
                            # Rate limiting
                            if REQUEST_DELAY > 0:
                                await asyncio.sleep(REQUEST_DELAY)
                            
                            return data
                        elif response.status == 429:
                            # Rate limited - wait longer
                            wait_time = RETRY_DELAY * (2 ** attempt)
                            self.logger.warning(f"Rate limited on {url}. Waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            self.logger.warning(f"HTTP {response.status} for {url}")
                            
                except aiohttp.ClientError as e:
                    self.logger.error(f"Client error for {url}: {e}")
                except asyncio.TimeoutError:
                    self.logger.error(f"Timeout for {url}")
                except Exception as e:
                    self.logger.error(f"Unexpected error for {url}: {e}")
                
                # Retry delay (except for last attempt)
                if attempt < retries:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    self.logger.info(f"Retrying {url} in {wait_time}s")
                    await asyncio.sleep(wait_time)
            
            self.failed_requests += 1
            self.logger.error(f"Failed to fetch {url} after {retries + 1} attempts")
            return None
    
    async def fetch_multiple_pages(self, base_url: str, max_pages: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch multiple pages of JSON data concurrently
        
        Args:
            base_url: Base URL for the API endpoint
            max_pages: Maximum number of pages to fetch
            
        Returns:
            List of JSON responses from all pages
        """
        # First, fetch page 1 to check if there are more pages
        first_page = await self.fetch_json(base_url)
        if not first_page or 'products' not in first_page:
            return []
        
        all_responses = [first_page]
        
        # If there are products on the first page, check for more pages
        if len(first_page.get('products', [])) > 0:
            # Create tasks for additional pages
            tasks = []
            for page_num in range(2, max_pages + 1):
                page_url = f"{base_url}?page={page_num}"
                tasks.append(self.fetch_json(page_url))
            
            # Fetch all pages concurrently
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, dict) and 'products' in result:
                        products = result.get('products', [])
                        if products:  # Only add if there are products
                            all_responses.append(result)
                        else:
                            # No more products, stop
                            break
                    elif isinstance(result, Exception):
                        self.logger.error(f"Error fetching page: {result}")
        
        return all_responses
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraping session statistics"""
        return {
            "total_requests": self.request_count,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": (self.successful_requests / max(self.request_count, 1)) * 100
        }


def ensure_output_directory(directory: str) -> None:
    """Ensure output directory exists"""
    if not directory:
        raise ValueError("directory path must be provided")

    try:
        os.makedirs(directory, exist_ok=True)
    except PermissionError as exc:
        raise PermissionError(f"Unable to create output directory '{directory}'") from exc
    except OSError as exc:
        raise OSError(f"Failed to create output directory '{directory}': {exc}") from exc


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
        
        with open(filepath, 'w', encoding='utf-8') as f:
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
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.warning(f"File not found: {filepath}")
        return None
    except Exception as e:
        logging.error(f"Error loading JSON from {filepath}: {e}")
        return None


def build_product_url(handle: str) -> str:
    """Build full product URL from handle"""
    return urljoin(BASE_URL, f"/products/{handle}")


def extract_category_from_url(url: str) -> str:
    """Extract category name from collection URL"""
    try:
        # Parse URL and extract category from path
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')
        
        # Find 'collections' in path and get the next part
        if 'collections' in path_parts:
            idx = path_parts.index('collections')
            if idx + 1 < len(path_parts):
                return path_parts[idx + 1]
        
        return "unknown"
    except Exception:
        return "unknown"


def format_price(price_str: str) -> str:
    """Format price string to ensure consistency"""
    if not price_str:
        return "0.00"
    
    # Remove any currency symbols and extra whitespace
    cleaned = str(price_str).replace(',', '').strip()
    
    try:
        # Convert to float and back to ensure proper format
        price_float = float(cleaned)
        return f"{price_float:.2f}"
    except ValueError:
        return cleaned


def get_availability_text(available: bool) -> str:
    """Convert boolean availability to text"""
    return "In Stock" if available else "Out of Stock"


def setup_logging(log_file: str = "scraper.log", level: str = "INFO") -> None:
    """Setup logging configuration (console only, no file)"""
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Console handler only (no file logging)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Configure root logger with console handler only
    logging.basicConfig(
        level=log_level,
        handlers=[console_handler],
        force=True  # Override any existing configuration
    )


def create_backup_filename(original_filename: str) -> str:
    """Create backup filename with timestamp"""
    name, ext = os.path.splitext(original_filename)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{name}_backup_{timestamp}{ext}"


class ProgressTracker:
    """Simple progress tracking utility"""
    
    def __init__(self, total_items: int, description: str = "Processing"):
        self.total_items = total_items
        self.processed_items = 0
        self.description = description
        self.start_time = time.time()
        self.logger = logging.getLogger(__name__)
    
    def update(self, increment: int = 1) -> None:
        """Update progress"""
        self.processed_items += increment
        self.log_progress()
    
    def log_progress(self) -> None:
        """Log current progress"""
        if self.total_items > 0:
            percentage = (self.processed_items / self.total_items) * 100
            elapsed = time.time() - self.start_time
            
            if self.processed_items > 0:
                estimated_total = elapsed * self.total_items / self.processed_items
                remaining = estimated_total - elapsed
                
                self.logger.info(
                    f"{self.description}: {self.processed_items}/{self.total_items} "
                    f"({percentage:.1f}%) - ETA: {remaining:.1f}s"
                )
    
    def finish(self) -> None:
        """Log completion"""
        elapsed = time.time() - self.start_time
        self.logger.info(
            f"{self.description} completed: {self.processed_items}/{self.total_items} "
            f"in {elapsed:.2f}s"
        )