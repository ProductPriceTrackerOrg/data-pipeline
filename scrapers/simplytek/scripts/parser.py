import re
import json
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from scrapers.simplytek.models import Product, Variant, Metadata
from datetime import datetime


class SimplyTekParser:
    def __init__(self, base_url: str = "https://www.simplytek.lk"):
        self.base_url = base_url

    def parse_product_cards(self, html: str) -> List[str]:
        """Extract product URLs from collection page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check if collection is empty
        if soup.find('div', class_='collection-empty'):
            return []
        
        product_links = []
        product_cards = soup.find_all('product-card')
        
        for card in product_cards:
            link = card.find('a', class_='product-featured-image-link')
            if link and link.get('href'):
                full_url = urljoin(self.base_url, link['href'])
                product_links.append(full_url)
        
        return product_links

    def parse_product_page(self, html: str, product_url: str) -> Optional[Product]:
        """Parse individual product page"""
        soup = BeautifulSoup(html, 'html.parser')
        
        try:
            # Extract product ID from JSON script
            variant_script = soup.find('script', string=re.compile(r'\[{.*"id":\d+.*}\]'))
            if not variant_script:
                return None
            
            variant_data = self._extract_variant_json(variant_script.string)
            if not variant_data:
                return None

            # Extract basic product info
            product_title = self._extract_title(soup)
            if not product_title:
                return None

            product_id = self._extract_product_id(soup)
            description = self._extract_description(soup)
            brand = self._extract_brand(soup)
            category_path = self._extract_category_path(product_url)
            image_urls = self._extract_images(soup)
            variants = self._parse_variants(variant_data, product_title, soup)

            if not variants:
                return None

            return Product(
                product_id_native=product_id,
                product_url=product_url,
                product_title=product_title,
                description_html=description,
                brand=brand,
                category_path=category_path,
                image_urls=image_urls,
                variants=variants,
                metadata=Metadata(scrape_timestamp=datetime.now())
            )
        
        except Exception as e:
            print(f"Error parsing product {product_url}: {str(e)}")
            return None

    def _extract_variant_json(self, script_content: str) -> Optional[List[Dict]]:
        """Extract variant JSON data from script tag"""
        try:
            json_match = re.search(r'\[{.*}\]', script_content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
        except json.JSONDecodeError:
            pass
        return None

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract product title"""
        title_elem = soup.find('h1', class_='product-title')
        return title_elem.get_text(strip=True) if title_elem else None

    def _extract_product_id(self, soup: BeautifulSoup) -> str:
        """Extract product ID from hidden input"""
        product_id_input = soup.find('input', {'name': 'product-id'})
        if product_id_input and product_id_input.get('value'):
            return product_id_input['value']
        return "unknown"

    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract product description"""
        desc_elem = soup.find('div', class_='collapsible__content')
        return str(desc_elem) if desc_elem else None

    def _extract_brand(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract brand from product title (first word)"""
        title_elem = soup.find('h1', class_='product-title')
        if title_elem:
            title = title_elem.get_text(strip=True)
            return title.split()[0] if title else None
        return None

    def _extract_category_path(self, product_url: str) -> List[str]:
        """Extract category from URL path"""
        parsed_url = urlparse(product_url)
        path_parts = [part for part in parsed_url.path.split('/') if part]
        
        # Remove 'collections', 'all', 'products' from path
        filtered_parts = [part for part in path_parts 
                         if part not in ['collections', 'all', 'products']]
        return filtered_parts if filtered_parts else ['all']

    def _extract_images(self, soup: BeautifulSoup) -> List[str]:
        """Extract product image URLs"""
        image_urls = []
        
        # Find all product images
        img_elements = soup.find_all('img', {'data-srcset': True})
        
        for img in img_elements:
            srcset = img.get('data-srcset', '')
            if 'simplytek.lk/cdn/shop/files/' in srcset:
                # Extract highest resolution image
                urls = re.findall(r'(//[^\s]+)\s+\d+w', srcset)
                if urls:
                    full_url = 'https:' + urls[-1].split('&width=')[0]
                    if full_url not in image_urls:
                        image_urls.append(full_url)
        
        return image_urls[:5]  # Limit to 5 images

    def _parse_variants(self, variant_data: List[Dict], product_title: str, 
                       soup: BeautifulSoup) -> List[Variant]:
        """Parse product variants"""
        variants = []
        
        for variant in variant_data:
            try:
                variant_id = str(variant.get('id', ''))
                variant_title = variant.get('title', product_title)
                
                # If no variant title or same as main title, use product title
                if not variant_title or variant_title == product_title:
                    variant_title = product_title
                
                price_current = self._format_price(variant.get('price', 0))
                price_original = None
                compare_price = variant.get('compare_at_price')
                if compare_price and compare_price > variant.get('price', 0):
                    price_original = self._format_price(compare_price)
                
                availability = "In Stock" if variant.get('available', False) else "Out of Stock"
                
                variants.append(Variant(
                    variant_id_native=variant_id,
                    variant_title=variant_title,
                    price_current=price_current,
                    price_original=price_original,
                    currency="LKR",
                    availability_text=availability
                ))
            except Exception as e:
                print(f"Error parsing variant: {e}")
                continue
        
        return variants

    def _format_price(self, price_cents: int) -> str:
        """Format price from cents to LKR string"""
        if price_cents == 0:
            return "Rs 0.00"
        price_lkr = price_cents / 100
        return f"Rs {price_lkr:,.2f}"