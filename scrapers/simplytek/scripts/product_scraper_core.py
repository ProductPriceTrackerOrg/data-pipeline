"""
Core product scraping implementation
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional, Set
from datetime import datetime

from models.product_models import (
    Product, ProductVariant, ProductMetadata, ScrapingResult,
    ShopifyProduct, ShopifyVariant, ShopifyApiResponse
)
from utils.scraper_utils import (
    ScrapingSession, build_product_url, format_price, 
    get_availability_text, ProgressTracker
)
from config.scraper_config import (
    CATEGORY_URLS, ALL_PRODUCTS_URL, SHOP_METADATA, 
    DEFAULT_CURRENCY, CATEGORY_PATH_MAPPING, BASE_URL
)


class ProductScraper:
    """Main product scraper class"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.scraped_products: List[Product] = []
        self.scraped_product_ids: Set[str] = set()
        self.scraping_stats = {
            "start_time": None,
            "end_time": None,
            "categories_processed": 0,
            "products_scraped": 0,
            "variants_scraped": 0,
            "errors": []
        }
    
    async def scrape_all_products(self) -> ScrapingResult:
        """
        Scrape all products from all categories
        
        Returns:
            ScrapingResult containing all scraped products and metadata
        """
        self.logger.info("Starting comprehensive product scraping")
        self.scraping_stats["start_time"] = datetime.now()
        
        async with ScrapingSession() as session:
            # Strategy 1: Try to scrape from the all products endpoint first
            all_products = await self._scrape_all_products_endpoint(session)
            
            if all_products:
                self.logger.info(f"Successfully scraped {len(all_products)} products from all-products endpoint")
                self.scraped_products = all_products
            else:
                # Strategy 2: Scrape category by category if all-products fails
                self.logger.info("Falling back to category-by-category scraping")
                await self._scrape_by_categories(session)
            
            # Get session stats
            session_stats = session.get_stats()
            self.logger.info(f"Session stats: {session_stats}")
            
        self.scraping_stats["end_time"] = datetime.now()
        self.scraping_stats["products_scraped"] = len(self.scraped_products)
        self.scraping_stats["variants_scraped"] = sum(
            len(product.variants) for product in self.scraped_products
        )
        
        # Create result
        result = ScrapingResult(
            total_products=len(self.scraped_products),
            total_variants=self.scraping_stats["variants_scraped"],
            categories_scraped=list(CATEGORY_URLS.keys()),
            scraping_metadata={
                "scraping_duration": str(self.scraping_stats["end_time"] - self.scraping_stats["start_time"]),
                "session_stats": session_stats,
                "scraping_stats": self.scraping_stats
            },
            products=self.scraped_products
        )
        
        self.logger.info(f"Scraping completed: {result.total_products} products, {result.total_variants} variants")
        return result
    
    async def _scrape_all_products_endpoint(self, session: ScrapingSession) -> List[Product]:
        """Scrape from the all-products endpoint with pagination"""
        self.logger.info("Attempting to scrape from all-products endpoint")
        
        try:
            # Fetch all pages from the all-products endpoint
            all_pages = await session.fetch_multiple_pages(ALL_PRODUCTS_URL, max_pages=50)
            
            if not all_pages:
                self.logger.warning("No data received from all-products endpoint")
                return []
            
            # Process all products from all pages
            all_products = []
            total_raw_products = 0
            
            for page_data in all_pages:
                raw_products = page_data.get('products', [])
                total_raw_products += len(raw_products)
                
                for raw_product in raw_products:
                    try:
                        # Transform Shopify product to our format
                        product = await self._transform_shopify_product(
                            raw_product, category="all-products"
                        )
                        if product and product.product_id_native not in self.scraped_product_ids:
                            all_products.append(product)
                            self.scraped_product_ids.add(product.product_id_native)
                            
                    except Exception as e:
                        self.logger.error(f"Error processing product {raw_product.get('id', 'unknown')}: {e}")
                        self.scraping_stats["errors"].append(str(e))
            
            self.logger.info(f"Processed {total_raw_products} raw products into {len(all_products)} products")
            return all_products
            
        except Exception as e:
            self.logger.error(f"Error scraping all-products endpoint: {e}")
            self.scraping_stats["errors"].append(f"All-products endpoint error: {e}")
            return []
    
    async def _scrape_by_categories(self, session: ScrapingSession) -> None:
        """Scrape products category by category"""
        self.logger.info("Starting category-by-category scraping")
        
        progress = ProgressTracker(len(CATEGORY_URLS), "Scraping categories")
        
        # Create tasks for concurrent category scraping
        tasks = []
        for category, url in CATEGORY_URLS.items():
            task = self._scrape_category(session, category, url)
            tasks.append(task)
        
        # Execute category scraping concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, result in enumerate(results):
            category = list(CATEGORY_URLS.keys())[i]
            
            if isinstance(result, Exception):
                self.logger.error(f"Error scraping category {category}: {result}")
                self.scraping_stats["errors"].append(f"Category {category}: {result}")
            else:
                category_products = result if result else []
                self.logger.info(f"Category {category}: {len(category_products)} products")
                
                # Add products, avoiding duplicates
                for product in category_products:
                    if product.product_id_native not in self.scraped_product_ids:
                        self.scraped_products.append(product)
                        self.scraped_product_ids.add(product.product_id_native)
            
            progress.update()
            self.scraping_stats["categories_processed"] += 1
        
        progress.finish()
    
    async def _scrape_category(self, session: ScrapingSession, category: str, url: str) -> List[Product]:
        """Scrape products from a specific category"""
        self.logger.info(f"Scraping category: {category}")
        
        try:
            # Fetch all pages for this category
            all_pages = await session.fetch_multiple_pages(url, max_pages=20)
            
            if not all_pages:
                self.logger.warning(f"No data received for category: {category}")
                return []
            
            category_products = []
            
            # Process products from all pages
            for page_data in all_pages:
                raw_products = page_data.get('products', [])
                
                for raw_product in raw_products:
                    try:
                        product = await self._transform_shopify_product(raw_product, category)
                        if product:
                            category_products.append(product)
                            
                    except Exception as e:
                        self.logger.error(f"Error processing product in category {category}: {e}")
                        self.scraping_stats["errors"].append(f"Category {category} product error: {e}")
            
            self.logger.info(f"Category {category} completed: {len(category_products)} products")
            return category_products
            
        except Exception as e:
            self.logger.error(f"Error scraping category {category}: {e}")
            self.scraping_stats["errors"].append(f"Category {category} error: {e}")
            return []
    
    async def _transform_shopify_product(self, raw_product: Dict[str, Any], category: str) -> Optional[Product]:
        """
        Transform raw Shopify product data to our Product model
        
        Args:
            raw_product: Raw product data from Shopify API
            category: Category name for building category path
            
        Returns:
            Transformed Product object or None if transformation fails
        """
        try:
            # Basic product information
            product_id = str(raw_product.get('id', ''))
            product_title = raw_product.get('title', '').strip()
            product_handle = raw_product.get('handle', '').strip()
            
            if not product_id or not product_title:
                self.logger.warning(f"Missing required product data: ID={product_id}, Title={product_title}")
                return None
            
            # Build product URL
            product_url = build_product_url(product_handle)
            
            # Description
            description_html = raw_product.get('body_html')
            
            # Brand (from vendor)
            brand = raw_product.get('vendor')
            
            # Category path
            category_path = self._build_category_path(category)
            
            # Image URLs
            image_urls = self._extract_image_urls(raw_product.get('images', []))
            
            # Process variants
            variants = self._transform_variants(raw_product.get('variants', []), product_id, product_title)
            
            if not variants:
                self.logger.warning(f"No valid variants found for product {product_id}")
                return None
            
            # Create metadata
            metadata = ProductMetadata(
                source_website=SHOP_METADATA["source_website"],
                shop_contact_phone=SHOP_METADATA["shop_contact_phone"],
                shop_contact_whatsapp=SHOP_METADATA["shop_contact_whatsapp"],
                scrape_timestamp=datetime.now()
            )
            
            # Create and return product
            product = Product(
                product_id_native=product_id,
                product_url=product_url,
                product_title=product_title,
                description_html=description_html,
                brand=brand,
                category_path=category_path,
                image_urls=image_urls,
                variants=variants,
                metadata=metadata
            )
            
            return product
            
        except Exception as e:
            self.logger.error(f"Error transforming product {raw_product.get('id', 'unknown')}: {e}")
            return None
    
    def _transform_variants(self, raw_variants: List[Dict[str, Any]], 
                          product_id: str, product_title: str) -> List[ProductVariant]:
        """Transform raw Shopify variants to ProductVariant objects"""
        variants = []
        
        for raw_variant in raw_variants:
            try:
                variant_id = str(raw_variant.get('id', ''))
                variant_title = raw_variant.get('title', '').strip()
                
                # If variant title is "Default Title", use product title
                if not variant_title or variant_title.lower() in ['default title', 'default']:
                    variant_title = product_title
                
                # Pricing
                price_current = format_price(raw_variant.get('price', '0'))
                price_original = None
                compare_at_price = raw_variant.get('compare_at_price')
                
                if compare_at_price:
                    price_original = format_price(compare_at_price)
                
                # Availability
                available = raw_variant.get('available', False)
                availability_text = get_availability_text(available)
                
                # Create variant
                variant = ProductVariant(
                    variant_id_native=variant_id,
                    variant_title=variant_title,
                    price_current=price_current,
                    price_original=price_original,
                    currency=DEFAULT_CURRENCY,
                    availability_text=availability_text
                )
                
                variants.append(variant)
                
            except Exception as e:
                self.logger.error(f"Error transforming variant {raw_variant.get('id', 'unknown')}: {e}")
                continue
        
        return variants
    
    def _extract_image_urls(self, raw_images: List[Dict[str, Any]]) -> List[str]:
        """Extract image URLs from raw image data"""
        image_urls = []
        
        for image in raw_images:
            src = image.get('src')
            if src and isinstance(src, str):
                # Clean and validate URL
                src = src.strip()
                if src.startswith('http'):
                    image_urls.append(src)
        
        return image_urls
    
    def _build_category_path(self, category: str) -> List[str]:
        """Build category path from category name"""
        # Use predefined mapping or create a default path
        if category in CATEGORY_PATH_MAPPING:
            return CATEGORY_PATH_MAPPING[category].copy()
        
        # Create default category path
        category_clean = category.replace('-', ' ').title()
        return ["Electronics", category_clean]
    
    def get_scraping_summary(self) -> Dict[str, Any]:
        """Get summary of scraping results"""
        return {
            "total_products": len(self.scraped_products),
            "total_variants": sum(len(product.variants) for product in self.scraped_products),
            "unique_categories": len(set(
                tuple(product.category_path) for product in self.scraped_products
            )),
            "unique_brands": len(set(
                product.brand for product in self.scraped_products if product.brand
            )),
            "scraping_stats": self.scraping_stats
        }