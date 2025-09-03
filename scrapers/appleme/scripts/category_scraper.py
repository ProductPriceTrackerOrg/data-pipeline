"""
Category scraper for extracting product categories and product lists
"""
import asyncio
import re
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

from config.scraper_config import ScraperConfig, SelectorConfig
from utils.scraper_utils import AsyncRequestManager, ScraperUtils


class CategoryScraper:
    """Scrapes product categories and product lists"""
    
    def __init__(self):
        self.logger = ScraperUtils.setup_logging()
        self.utils = ScraperUtils()
    
    async def get_main_categories(self, request_manager: AsyncRequestManager) -> List[Dict[str, str]]:
        """Get all main categories from the shop page navigation menu"""
        self.logger.info("Fetching main categories...")
        
        html = await request_manager.fetch(ScraperConfig.CATEGORIES_URL)
        if not html:
            self.logger.error("Failed to fetch shop page")
            return []
        
        soup = request_manager.get_soup(html)
        categories = []
        
        # Find main category menu items (not submenus)
        main_menu_items = soup.select("ul#menu-leftmenu > li.menu-item-object-product_cat")
        
        for item in main_menu_items:
            # Get direct child anchor link only (not submenu links)
            link = item.find('a', recursive=False)
            
            if link:
                category_url = link.get('href', '')
                category_name = self.utils.clean_text(link.get_text())
                
                # For navigation menu, we don't have product counts readily available
                # We'll set count to 1 as a placeholder since these are valid categories
                if category_url and category_name:
                    categories.append({
                        'name': category_name,
                        'url': self.utils.normalize_url(category_url),
                        'count': 1  # Placeholder count for menu-based categories
                    })
        
        self.logger.info(f"Found {len(categories)} main categories")
        return categories
    
    async def get_category_products(self, request_manager: AsyncRequestManager, 
                                 category_url: str, category_name: str) -> List[Dict[str, str]]:
        """Get all products from a category (including pagination)"""
        self.logger.info(f"Scraping products from category: {category_name}")
        
        products = []
        page = 1
        
        while True:
            # Construct page URL
            if page == 1:
                page_url = category_url
            else:
                page_url = f"{category_url.rstrip('/')}/page/{page}/"
            
            self.logger.info(f"Fetching page {page} of {category_name}")
            html = await request_manager.fetch(page_url)
            
            if not html:
                self.logger.warning(f"Failed to fetch page {page} of {category_name}")
                break
            
            soup = request_manager.get_soup(html)
            page_products = self._extract_products_from_page(soup, category_name)
            
            if not page_products:
                self.logger.info(f"No more products found in {category_name} at page {page}")
                break
            
            products.extend(page_products)
            self.logger.info(f"Found {len(page_products)} products on page {page} of {category_name}")
            
            # Check if there's a next page
            if not self._has_next_page(soup):
                break
            
            page += 1
        
        self.logger.info(f"Total products found in {category_name}: {len(products)}")
        return products
    
    def _extract_products_from_page(self, soup, category_name: str) -> List[Dict[str, str]]:
        """Extract product information from a category page"""
        products = []
        product_items = soup.select(SelectorConfig.PRODUCT_ITEMS)
        
        for item in product_items:
            try:
                # Product link and title
                product_link = item.select_one(SelectorConfig.PRODUCT_LINK)
                if not product_link:
                    continue
                
                product_url = product_link.get('href', '')
                if not product_url:
                    continue
                
                # Product title
                title_element = item.select_one(SelectorConfig.PRODUCT_TITLE)
                product_title = self.utils.clean_text(title_element.get_text()) if title_element else ""
                
                # Product image
                image_element = item.select_one(SelectorConfig.PRODUCT_IMAGE)
                image_url = ""
                if image_element:
                    image_url = image_element.get('src') or image_element.get('data-src') or ""
                    image_url = self.utils.normalize_url(image_url)
                
                # Price
                price_element = item.select_one(SelectorConfig.PRODUCT_PRICE)
                price = self.utils.extract_price(price_element.get_text()) if price_element else "0.00"
                
                # Stock status
                stock_element = item.select_one(SelectorConfig.STOCK_STATUS)
                availability = self.utils.clean_text(stock_element.get_text()) if stock_element else "Unknown"
                
                # Product ID - try to extract from various sources
                product_id = self._extract_product_id_from_item(item)
                
                products.append({
                    'product_id': product_id,
                    'title': product_title,
                    'url': self.utils.normalize_url(product_url),
                    'image_url': image_url,
                    'price': price,
                    'availability': availability,
                    'category': category_name
                })
                
            except Exception as e:
                self.logger.warning(f"Error extracting product from item: {e}")
                continue
        
        return products
    
    def _extract_product_id_from_item(self, item) -> str:
        """Extract product ID from product item"""
        # Try add-to-cart button
        add_to_cart = item.select_one('a.add_to_cart_button')
        if add_to_cart:
            product_id = add_to_cart.get('data-product_id')
            if product_id:
                return product_id
        
        # Try product link URL
        product_link = item.select_one(SelectorConfig.PRODUCT_LINK)
        if product_link:
            url = product_link.get('href', '')
            match = re.search(r'/product/([^/]+)/?', url)
            if match:
                return match.group(1).replace('-', '_')
        
        # Generate fallback ID
        import time
        return f"unknown_{int(time.time() * 1000)}"
    
    def _has_next_page(self, soup) -> bool:
        """Check if there's a next page"""
        next_link = soup.select_one(SelectorConfig.NEXT_PAGE)
        return next_link is not None
    
    async def scrape_all_categories(self, request_manager: AsyncRequestManager) -> Dict[str, List[Dict[str, str]]]:
        """Scrape all categories and their products"""
        # Get main categories
        categories = await self.get_main_categories(request_manager)
        if not categories:
            return {}
        
        # Create semaphore to limit concurrent category scraping
        semaphore = asyncio.Semaphore(ScraperConfig.MAX_CONCURRENT_CATEGORIES)
        
        async def scrape_category(category):
            async with semaphore:
                return await self.get_category_products(
                    request_manager, 
                    category['url'], 
                    category['name']
                )
        
        # Scrape all categories concurrently
        self.logger.info(f"Starting to scrape {len(categories)} categories...")
        tasks = [scrape_category(cat) for cat in categories]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Organize results
        category_products = {}
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Error scraping category {categories[i]['name']}: {result}")
                continue
            
            category_name = categories[i]['name']
            category_products[category_name] = result
        
        return category_products