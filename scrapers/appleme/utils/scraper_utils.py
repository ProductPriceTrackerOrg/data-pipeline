"""
Utility functions for the scraper
"""
import re
import time
import json
import logging
import asyncio
import aiohttp
import random
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime

from config.scraper_config import ScraperConfig


class ScraperUtils:
    """Utility functions for web scraping"""
    
    @staticmethod
    def setup_logging() -> logging.Logger:
        """Setup logging configuration - console only"""
        logger = logging.getLogger('appleme_scraper')
        logger.setLevel(logging.INFO)
        
        # Clear any existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Create console handler only
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(console_handler)
        
        return logger
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        if not logger.handlers:
            logger.addHandler(file_handler)
            logger.addHandler(console_handler)
        
        return logger
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())
    
    @staticmethod
    def extract_price(price_text: str) -> str:
        """Extract price from text"""
        if not price_text:
            return "0.00"
        
        # Remove currency symbols and extract numbers
        price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
        if price_match:
            return price_match.group()
        return "0.00"
    
    @staticmethod
    def extract_product_id(element) -> str:
        """Extract product ID from various sources"""
        # Try to get from add-to-cart button
        add_to_cart = element.find('button', {'name': 'add-to-cart'})
        if add_to_cart and add_to_cart.get('value'):
            return add_to_cart['value']
        
        # Try to get from data attributes
        product_link = element.find('a', class_='woocommerce-LoopProduct-link')
        if product_link:
            url = product_link.get('href', '')
            # Extract product ID from URL
            match = re.search(r'/product/([^/]+)/?', url)
            if match:
                return match.group(1)
        
        # Generate from timestamp if nothing found
        return f"unknown_{int(time.time())}"
    
    @staticmethod
    def normalize_url(url: str, base_url: str = ScraperConfig.BASE_URL) -> str:
        """Normalize and join URLs"""
        if not url:
            return ""
        return urljoin(base_url, url)
    
    @staticmethod
    def extract_category_path(breadcrumb_soup) -> List[str]:
        """Extract category path from breadcrumb"""
        if not breadcrumb_soup:
            return []
        
        links = breadcrumb_soup.find_all('a')
        categories = []
        
        for link in links:
            text = ScraperUtils.clean_text(link.get_text())
            # Skip 'Home' and empty categories
            if text and text.lower() != 'home':
                categories.append(text)
        
        return categories
    
    @staticmethod
    def extract_images(soup, selector: str) -> List[str]:
        """Extract image URLs from soup"""
        images = []
        img_elements = soup.select(selector)
        
        for img in img_elements:
            src = img.get('src') or img.get('data-src')
            if src:
                full_url = ScraperUtils.normalize_url(src)
                if full_url not in images:
                    images.append(full_url)
        
        return images
    
    @staticmethod
    def extract_brand(soup) -> Optional[str]:
        """Extract brand from product page"""
        brand_img = soup.select_one('div.pwb-single-product-brands img')
        if brand_img:
            alt_text = brand_img.get('alt', '')
            if alt_text:
                return ScraperUtils.clean_text(alt_text)
        
        # Try to extract from title or other sources
        title = soup.select_one('h1.product_title')
        if title:
            title_text = title.get_text()
            # Common brand patterns
            brands = ['Xiaomi', 'Samsung', 'Apple', 'Sony', 'LG', 'Huawei', 'OnePlus', 'Nokia']
            for brand in brands:
                if brand.lower() in title_text.lower():
                    return brand
        
        return None
    
    @staticmethod
    def save_json(data: Any, filename: str) -> bool:
        """Save data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            return True
        except Exception as e:
            logging.error(f"Error saving JSON to {filename}: {e}")
            return False
    
    @staticmethod
    def load_json(filename: str) -> Optional[Any]:
        """Load data from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading JSON from {filename}: {e}")
            return None


class AsyncRequestManager:
    """Manages async HTTP requests with rate limiting and retries"""
    
    def __init__(self):
        self.session = None
        self.logger = ScraperUtils.setup_logging()
        self.current_delay = ScraperConfig.MIN_DELAY  # Adaptive delay
        self.consecutive_429s = 0  # Track consecutive rate limit errors
        self.last_request_time = 0
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(
            limit=ScraperConfig.MAX_CONCURRENT_REQUESTS * 2,  # Increased connection pool
            limit_per_host=ScraperConfig.MAX_CONCURRENT_REQUESTS,
            ttl_dns_cache=600,  # DNS cache for 10 minutes
            use_dns_cache=True,
            enable_cleanup_closed=True,
            keepalive_timeout=30,  # Keep connections alive longer
            ssl=False  # Disable SSL verification for speed (if acceptable)
        )
        timeout = aiohttp.ClientTimeout(
            total=ScraperConfig.REQUEST_TIMEOUT,
            connect=5,  # Quick connection timeout
            sock_read=10  # Quick read timeout
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=ScraperConfig.get_headers(),
            skip_auto_headers=['User-Agent']  # We set it manually
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def fetch(self, url: str, retries: int = ScraperConfig.MAX_RETRIES) -> Optional[str]:
        """Fetch URL with adaptive rate limiting and retries"""
        if not self.session:
            raise RuntimeError("Session not initialized. Use async context manager.")
        
        for attempt in range(retries + 1):
            try:
                # Adaptive delay based on previous responses
                await self._adaptive_delay()
                
                async with self.session.get(url) as response:
                    if response.status == 429:
                        # Rate limited - increase delay and wait
                        self._handle_rate_limit()
                        self.logger.warning(f"Rate limited for {url}, waiting {ScraperConfig.RATE_LIMIT_DELAY}s")
                        await asyncio.sleep(ScraperConfig.RATE_LIMIT_DELAY)
                        continue
                    elif response.status == 200:
                        # Success - gradually reduce delay
                        self._handle_success()
                        content = await response.text()
                        return content
                    elif response.status in [403, 404]:
                        # Don't retry for these
                        self.logger.warning(f"HTTP {response.status} for {url} - not retrying")
                        return None
                    else:
                        self.logger.warning(f"HTTP {response.status} for {url}")
                        
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
            except Exception as e:
                self.logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            
            if attempt < retries:
                # Exponential backoff with jitter
                wait_time = ScraperConfig.RETRY_DELAY * (2 ** attempt) + random.uniform(0, 1)
                await asyncio.sleep(wait_time)
        
        self.logger.error(f"Failed to fetch {url} after {retries + 1} attempts")
        return None
    
    async def _adaptive_delay(self):
        """Apply minimal adaptive delay between requests"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        # Use very minimal delays for speed
        min_delay = max(self.current_delay, 0.05)  # Never less than 50ms
        
        # Ensure minimum time between requests
        if elapsed < min_delay:
            await asyncio.sleep(min_delay - elapsed)
        
        self.last_request_time = time.time()
    
    def _handle_rate_limit(self):
        """Handle rate limit response with quick recovery"""
        self.consecutive_429s += 1
        # Less aggressive backoff for faster recovery
        self.current_delay = min(self.current_delay * 1.2, 3.0)  # Cap at 3 seconds
        self.logger.info(f"Rate limit hit, increased delay to {self.current_delay:.2f}s")
    
    def _handle_success(self):
        """Handle successful response by quickly reducing delay"""
        if self.consecutive_429s > 0:
            self.consecutive_429s = max(0, self.consecutive_429s - 1)
        
        # Quickly reduce delay if no recent rate limits
        if self.consecutive_429s == 0:
            self.current_delay = max(self.current_delay * 0.9, ScraperConfig.MIN_DELAY)
    
    def get_soup(self, html: str) -> BeautifulSoup:
        """Create BeautifulSoup object from HTML"""
        return BeautifulSoup(html, 'html.parser')