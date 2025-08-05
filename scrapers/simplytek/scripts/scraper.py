import asyncio
import json
import time
from typing import List, Set
from pathlib import Path
import logging

# Use absolute imports instead of relative imports
from scrapers.simplytek.models import Product, ScrapingResult
# Import local modules with the dot prefix
from .parser import SimplyTekParser
from .http_client import RateLimitedHttpClient, RequestResult

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimplyTekScraper:
    def __init__(self, 
                 base_url: str = "https://www.simplytek.lk",
                 max_pages: int = 25,
                 concurrent_requests: int = 5):
        
        self.base_url = base_url
        self.max_pages = max_pages
        self.concurrent_requests = concurrent_requests
        self.parser = SimplyTekParser(base_url)
        
        # Tracking
        self.scraped_products: List[Product] = []
        self.errors: List[str] = []
        self.processed_urls: Set[str] = set()

    async def scrape_all_products(self) -> ScrapingResult:
        """Main scraping method"""
        start_time = time.time()
        logger.info("Starting SimplyTek scraping...")
        
        try:
            async with RateLimitedHttpClient(
                concurrent_requests=self.concurrent_requests,
                delay_between_requests=0.3,
                max_retries=3
            ) as client:
                
                # Step 1: Get all product URLs
                product_urls = await self._get_all_product_urls(client)
                logger.info(f"Found {len(product_urls)} product URLs")
                
                if not product_urls:
                    return ScrapingResult(
                        success=False,
                        products=[],
                        total_products=0,
                        pages_scraped=0,
                        errors=["No product URLs found"],
                        execution_time=time.time() - start_time
                    )
                
                # Step 2: Scrape products in batches
                await self._scrape_products_batch(client, product_urls)
                
                # Step 3: Generate results
                execution_time = time.time() - start_time
                success_rate = len(self.scraped_products) / len(product_urls) * 100
                
                logger.info(f"Scraping completed in {execution_time:.2f}s")
                logger.info(f"Successfully scraped {len(self.scraped_products)}/{len(product_urls)} products ({success_rate:.1f}%)")
                
                return ScrapingResult(
                    success=True,
                    products=self.scraped_products,
                    total_products=len(self.scraped_products),
                    pages_scraped=self._pages_scraped,
                    errors=self.errors,
                    execution_time=execution_time
                )
        
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            return ScrapingResult(
                success=False,
                products=self.scraped_products,
                total_products=len(self.scraped_products),
                pages_scraped=getattr(self, '_pages_scraped', 0),
                errors=self.errors + [str(e)],
                execution_time=time.time() - start_time
            )

    async def _get_all_product_urls(self, client: RateLimitedHttpClient) -> List[str]:
        """Get all product URLs from collection pages"""
        all_product_urls = []
        self._pages_scraped = 0
        
        # Generate page URLs
        page_urls = [f"{self.base_url}/collections/all"]
        page_urls.extend([
            f"{self.base_url}/collections/all?page={page}" 
            for page in range(2, self.max_pages + 1)
        ])
        
        # Fetch pages in batches of 3 to avoid overwhelming the server
        batch_size = 3
        for i in range(0, len(page_urls), batch_size):
            batch_urls = page_urls[i:i + batch_size]
            results = await client.fetch_multiple(batch_urls)
            
            for result in results:
                if result.success and result.content:
                    product_urls = self.parser.parse_product_cards(result.content)
                    if product_urls:
                        all_product_urls.extend(product_urls)
                        self._pages_scraped += 1
                        logger.info(f"Page {self._pages_scraped}: Found {len(product_urls)} products")
                    else:
                        # No products found, might have reached the end
                        logger.info(f"No products found on page, stopping pagination")
                        break
                else:
                    self.errors.append(f"Failed to fetch page: {result.url}")
            
            # Small delay between batches
            await asyncio.sleep(0.5)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in all_product_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)
        
        return unique_urls

    async def _scrape_products_batch(self, client: RateLimitedHttpClient, product_urls: List[str]):
        """Scrape products in batches"""
        batch_size = 10  # Process 10 products at a time
        total_batches = len(product_urls) // batch_size + (1 if len(product_urls) % batch_size else 0)
        
        for batch_num, i in enumerate(range(0, len(product_urls), batch_size), 1):
            batch_urls = product_urls[i:i + batch_size]
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_urls)} products)")
            
            # Fetch HTML for batch
            results = await client.fetch_multiple(batch_urls)
            
            # Parse products
            for result in results:
                if result.success and result.content:
                    product = self.parser.parse_product_page(result.content, result.url)
                    if product:
                        self.scraped_products.append(product)
                        logger.debug(f"✓ Scraped: {product.product_title}")
                    else:
                        self.errors.append(f"Failed to parse product: {result.url}")
                        logger.warning(f"✗ Parse failed: {result.url}")
                else:
                    error_msg = f"HTTP error for {result.url}: {result.error or result.status_code}"
                    self.errors.append(error_msg)
                    logger.warning(f"✗ {error_msg}")
            
            # Progress update
            progress = len(self.scraped_products)
            logger.info(f"Progress: {progress}/{len(product_urls)} products scraped")
            
            # Small delay between batches
            await asyncio.sleep(0.3)

    def save_results(self, filename: str = "simplytek_products.json"):
        """Save scraped products to JSON file"""
        if not self.scraped_products:
            logger.warning("No products to save")
            return
        
        # Convert to dict for JSON serialization
        products_data = [product.dict() for product in self.scraped_products]
        
        output_path = Path(filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(products_data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Saved {len(products_data)} products to {output_path}")

    async def scrape_sample(self, num_products: int = 5) -> ScrapingResult:
        """Scrape a small sample for testing"""
        logger.info(f"Scraping sample of {num_products} products...")
        
        async with RateLimitedHttpClient(concurrent_requests=2) as client:
            # Get first page only
            first_page_url = f"{self.base_url}/collections/all"
            result = await client.fetch_url(first_page_url)
            
            if not result.success:
                return ScrapingResult(
                    success=False,
                    products=[],
                    total_products=0,
                    pages_scraped=0,
                    errors=[f"Failed to fetch first page: {result.error}"],
                    execution_time=0
                )
            
            # Get sample URLs
            product_urls = self.parser.parse_product_cards(result.content)[:num_products]
            
            # Scrape sample
            await self._scrape_products_batch(client, product_urls)
            
            return ScrapingResult(
                success=True,
                products=self.scraped_products,
                total_products=len(self.scraped_products),
                pages_scraped=1,
                errors=self.errors,
                execution_time=0
            )
        
