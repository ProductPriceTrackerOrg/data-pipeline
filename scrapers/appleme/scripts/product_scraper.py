"""
Product detail scraper module for AppleMe.lk

This module contains classes for scraping detailed product information from
AppleMe.lk product pages. It handles both individual product scraping and 
batch processing with intelligent concurrency control and performance optimization.

Classes:
    ProductScraper: Handles individual product detail extraction
    ProductBatchScraper: Manages batch processing with adaptive concurrency
"""
import re
import asyncio
from typing import Dict, List, Optional
from datetime import datetime

from models.product_models import ProductModel, VariantModel, MetadataModel
from config.scraper_config import ScraperConfig, SelectorConfig
from utils.scraper_utils import AsyncRequestManager, ScraperUtils


class ProductScraper:
    """
    Scrapes detailed product information from individual product pages
    
    This class handles the extraction of comprehensive product data including:
    - Product details (title, description, images)
    - Pricing and variant information
    - Category classification
    - Brand information
    - Availability status
    """
    
    def __init__(self):
        self.logger = ScraperUtils.setup_logging()
        self.utils = ScraperUtils()
    
    async def scrape_product_detail(self, request_manager: AsyncRequestManager, 
                                  product_info: Dict[str, str]) -> Optional[ProductModel]:
        """
        Scrape detailed product information from a product page
        
        Args:
            request_manager: HTTP request manager for making requests
            product_info: Dictionary containing basic product information (url, title, etc.)
            
        Returns:
            ProductModel object with complete product data, or None if scraping failed
        """
        """Scrape detailed product information from product page"""
        product_url = product_info['url']
        self.logger.info(f"Scraping product: {product_info.get('title', 'Unknown')}")
        
        html = await request_manager.fetch(product_url)
        if not html:
            self.logger.error(f"Failed to fetch product page: {product_url}")
            return None
        
        soup = request_manager.get_soup(html)
        
        try:
            # Extract basic product information
            product_data = self._extract_basic_info(soup, product_info)
            
            # Extract category path from breadcrumb
            category_path = self._extract_category_path(soup)
            
            # Extract product images
            image_urls = self._extract_images(soup)
            
            # Extract brand
            brand = self._extract_brand(soup)
            
            # Extract description
            description_html = self._extract_description(soup)
            
            # Extract pricing and variant information
            variant_info = self._extract_variant_info(soup, product_info)
            
            # Create variant
            variant = VariantModel(
                variant_id_native=variant_info['variant_id'],
                variant_title=variant_info['variant_title'],
                price_current=variant_info['price_current'],
                price_original=variant_info['price_original'],
                currency=variant_info['currency'],
                availability_text=variant_info['availability_text']
            )
            
            # Create metadata
            metadata = MetadataModel(
                source_website=ScraperConfig.SOURCE_WEBSITE,
                shop_contact_phone=ScraperConfig.SHOP_CONTACT_PHONE,
                shop_contact_whatsapp=ScraperConfig.SHOP_CONTACT_WHATSAPP,
                scrape_timestamp=datetime.now()
            )
            
            # Create product model
            product = ProductModel(
                product_id_native=product_data['product_id'],
                product_url=product_url,
                product_title=product_data['product_title'],
                description_html=description_html,
                brand=brand,
                category_path=category_path,
                image_urls=image_urls,
                variants=[variant],
                metadata=metadata
            )
            
            return product
            
        except Exception as e:
            self.logger.error(f"Error scraping product {product_url}: {e}")
            return None
    
    def _extract_basic_info(self, soup, product_info: Dict[str, str]) -> Dict[str, str]:
        """Extract basic product information"""
        # Product title
        title_element = soup.select_one(SelectorConfig.PRODUCT_TITLE_DETAIL)
        product_title = self.utils.clean_text(title_element.get_text()) if title_element else product_info.get('title', '')
        
        # Product ID - try multiple methods
        product_id = self._extract_product_id(soup, product_info)
        
        return {
            'product_id': product_id,
            'product_title': product_title
        }
    
    def _extract_product_id(self, soup, product_info: Dict[str, str]) -> str:
        """Extract product ID using multiple methods"""
        # Method 1: From add-to-cart button
        add_to_cart = soup.select_one(SelectorConfig.PRODUCT_ID)
        if add_to_cart and add_to_cart.get('value'):
            return add_to_cart['value']
        
        # Method 2: From URL
        url = product_info.get('url', '')
        match = re.search(r'/product/([^/]+)/?', url)
        if match:
            return match.group(1).replace('-', '_')
        
        # Method 3: From existing product info
        if product_info.get('product_id'):
            return product_info['product_id']
        
        # Fallback: Generate from title
        title = product_info.get('title', 'unknown')
        clean_title = re.sub(r'[^a-zA-Z0-9]', '_', title.lower())
        return f"{clean_title[:50]}_{int(datetime.now().timestamp())}"
    
    def _extract_category_path(self, soup) -> List[str]:
        """Extract category path from breadcrumb navigation"""
        breadcrumb = soup.select_one("nav.woocommerce-breadcrumb")
        if not breadcrumb:
            return []
        
        return self.utils.extract_category_path(breadcrumb)
    
    def _extract_images(self, soup) -> List[str]:
        """Extract product images"""
        images = []
        
        # Main product images from gallery
        gallery_images = soup.select("div.woocommerce-product-gallery img")
        for img in gallery_images:
            src = img.get('src') or img.get('data-src') or img.get('data-large_image')
            if src:
                full_url = self.utils.normalize_url(src)
                if full_url not in images:
                    images.append(full_url)
        
        # Thumbnail images
        thumb_images = soup.select("ol.flex-control-thumbs img")
        for img in thumb_images:
            src = img.get('src')
            if src:
                full_url = self.utils.normalize_url(src)
                if full_url not in images:
                    images.append(full_url)
        
        return images
    
    def _extract_brand(self, soup) -> Optional[str]:
        """Extract brand information"""
        return self.utils.extract_brand(soup)
    
    def _extract_description(self, soup) -> Optional[str]:
        """Extract product description HTML"""
        desc_element = soup.select_one(SelectorConfig.PRODUCT_DESCRIPTION)
        if desc_element:
            return str(desc_element)
        return None
    
    def _extract_variant_info(self, soup, product_info: Dict[str, str]) -> Dict[str, str]:
        """Extract variant information (price, availability, etc.)"""
        # Extract current price
        price_current = self._extract_current_price(soup, product_info)
        
        # Extract original price (if discounted)
        price_original = self._extract_original_price(soup)
        
        # Extract availability
        availability_text = self._extract_availability(soup, product_info)
        
        # Since no variants mentioned, use product info as variant info
        variant_id = product_info.get('product_id', self._extract_product_id(soup, product_info))
        variant_title = product_info.get('title', '')
        
        return {
            'variant_id': variant_id,
            'variant_title': variant_title,
            'price_current': price_current,
            'price_original': price_original,
            'currency': 'LKR',  # Sri Lankan Rupees
            'availability_text': availability_text
        }
    
    def _extract_current_price(self, soup, product_info: Dict[str, str]) -> str:
        """Extract current selling price"""
        # Try different price selectors
        price_selectors = [
            "p.price .woocommerce-Price-amount",
            "div.payment-options div.cash strong",
            "span.electro-price .woocommerce-Price-amount"
        ]
        
        for selector in price_selectors:
            price_element = soup.select_one(selector)
            if price_element:
                price_text = price_element.get_text()
                price = self.utils.extract_price(price_text)
                if price != "0.00":
                    return price
        
        # Fallback to product_info price
        if product_info.get('price'):
            return self.utils.extract_price(product_info['price'])
        
        return "0.00"
    
    def _extract_original_price(self, soup) -> Optional[str]:
        """Extract original price if product is discounted"""
        # Look for discounted price elements
        discounted_element = soup.select_one("div.discounted-price")
        if discounted_element:
            original_price = self.utils.extract_price(discounted_element.get_text())
            if original_price != "0.00":
                return original_price
        
        # Look for struck-through prices - primary method
        # First try using the del tag directly
        del_elements = soup.select("del")
        for del_elem in del_elements:
            price_element = del_elem.select_one(".woocommerce-Price-amount")
            if price_element:
                original_price = self.utils.extract_price(price_element.get_text())
                if original_price != "0.00":
                    return original_price
        
        return None
    
    def _extract_availability(self, soup, product_info: Dict[str, str]) -> str:
        """Extract availability status"""
        # Try availability from product detail page
        availability_element = soup.select_one("div.availability p.stock")
        if availability_element:
            return self.utils.clean_text(availability_element.get_text())
        
        # Try stock status elements
        stock_elements = soup.select("p.stock")
        for stock_elem in stock_elements:
            stock_text = self.utils.clean_text(stock_elem.get_text())
            if stock_text:
                return stock_text
        
        # Fallback to product_info availability
        return product_info.get('availability', 'Unknown')


class ProductBatchScraper:
    """
    Handles batch scraping of multiple products with intelligent concurrency control
    
    This class manages the efficient processing of large product lists by:
    - Implementing adaptive concurrency based on success rates
    - Using intelligent batching to optimize memory usage
    - Monitoring performance metrics for real-time adjustments
    - Providing detailed progress reporting and error handling
    
    Features:
    - Dynamic concurrency scaling (15-20 requests based on performance)
    - Batch processing with configurable sizes
    - Success rate monitoring and adaptive adjustments
    - Memory-efficient processing of large product lists
    """
    
    def __init__(self):
        self.logger = ScraperUtils.setup_logging()
        self.product_scraper = ProductScraper()
        # Performance tracking metrics
        self.success_rate = 1.0  # Rolling success rate
        self.consecutive_successes = 0  # Track consecutive successful requests
        self.consecutive_failures = 0  # Track consecutive failed requests
    
    async def scrape_products_batch(self, request_manager: AsyncRequestManager,
                                  products_info: List[Dict[str, str]]) -> List[ProductModel]:
        """
        Scrape multiple products with intelligent batching and dynamic concurrency
        
        This method processes large lists of products efficiently by:
        1. Dividing products into manageable batches
        2. Adjusting concurrency based on real-time performance
        3. Monitoring success rates and adapting processing speed
        4. Providing detailed progress reporting
        
        Args:
            request_manager: HTTP request manager for making requests
            products_info: List of dictionaries containing product information
            
        Returns:
            List of successfully scraped ProductModel objects
        """
        initial_concurrency = ScraperConfig.MAX_CONCURRENT_REQUESTS
        batch_size = ScraperConfig.BATCH_SIZE
        
        self.logger.info(f"Starting intelligent batch scrape of {len(products_info)} products...")
        self.logger.info(f"Initial concurrency: {initial_concurrency}, Batch size: {batch_size}")
        
        results = []
        
        # Process products in batches for better memory management and adaptive performance
        for batch_start in range(0, len(products_info), batch_size):
            batch_end = min(batch_start + batch_size, len(products_info))
            batch = products_info[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (len(products_info) + batch_size - 1) // batch_size
            
            self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} products)")
            
            # Dynamically adjust concurrency based on recent performance
            current_concurrency = self._get_adaptive_concurrency(initial_concurrency)
            batch_results = await self._process_batch_adaptive(request_manager, batch, current_concurrency)
            
            results.extend(batch_results)
            
            # Update performance metrics for next batch optimization
            batch_success_rate = len(batch_results) / len(batch) if batch else 0
            self._update_performance_metrics(batch_success_rate)
            
            # Brief pause between batches to be respectful to the server
            if batch_num < total_batches:
                await asyncio.sleep(0.2)
            
            self.logger.info(f"Batch {batch_num} completed: {len(batch_results)}/{len(batch)} products (Rate: {batch_success_rate:.1%})")
        
        final_success_rate = len(results) / len(products_info) if products_info else 0
        self.logger.info(f"All batches completed! Successfully scraped {len(results)}/{len(products_info)} products (Final rate: {final_success_rate:.1%})")
        return results
    
    async def _process_batch_adaptive(self, request_manager: AsyncRequestManager, 
                                    batch: List[Dict[str, str]], concurrency: int) -> List[ProductModel]:
        """Process a batch with adaptive concurrency"""
        semaphore = asyncio.Semaphore(concurrency)
        
        async def scrape_single_product(product_info):
            async with semaphore:
                try:
                    result = await self.product_scraper.scrape_product_detail(request_manager, product_info)
                    if result:
                        self.consecutive_successes += 1
                        self.consecutive_failures = 0
                        return result
                    else:
                        self.consecutive_failures += 1
                        self.consecutive_successes = 0
                        return None
                except Exception as e:
                    self.consecutive_failures += 1
                    self.consecutive_successes = 0
                    self.logger.error(f"Error scraping {product_info.get('title', 'unknown')}: {e}")
                    return None
        
        # Create tasks for all products in batch
        tasks = [scrape_single_product(product_info) for product_info in batch]
        
        # Execute all tasks and filter successful results
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful_results = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        return successful_results
    
    def _get_adaptive_concurrency(self, base_concurrency: int) -> int:
        """
        Calculate adaptive concurrency based on recent performance metrics
        
        This method dynamically adjusts the number of concurrent requests based on:
        - Recent success/failure patterns
        - Overall success rate trends
        - Consecutive success/failure counts
        
        Args:
            base_concurrency: The baseline concurrency level
            
        Returns:
            Adjusted concurrency level (2-20 range)
        """
        if self.consecutive_successes >= 20:
            # Increase concurrency when performing well consistently
            return min(base_concurrency * 2, 20)
        elif self.consecutive_failures >= 5:
            # Decrease concurrency when encountering issues
            return max(base_concurrency // 2, 2)
        else:
            return base_concurrency
    
    def _update_performance_metrics(self, batch_success_rate: float):
        """
        Update rolling performance metrics for adaptive optimization
        
        Uses exponential moving average to track recent performance trends
        while giving more weight to recent results.
        
        Args:
            batch_success_rate: Success rate of the most recent batch (0.0-1.0)
        """
        # Update rolling success rate using exponential moving average
        # 80% weight to historical data, 20% to current batch
        self.success_rate = (self.success_rate * 0.8) + (batch_success_rate * 0.2)