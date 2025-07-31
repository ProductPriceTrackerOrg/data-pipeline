import json
import requests
import math
from datetime import datetime
import pandas as pd
import statistics
from typing import Dict, List, Any, Optional

class ProductDataExtractor:
    def __init__(self, type: str = "all"):
        self.base_url = "https://www.simplytek.lk"
        self.products_url = f"{self.base_url}/collections/{type}/products.json"
        self.collections_url = f"{self.base_url}/collections.json"
        self.scrape_timestamp = datetime.now().isoformat()
        
        # Shop contact information
        self.shop_phone = "+94 771990020"
        self.shop_whatsapp = "+94 771 990050"
        self.source_website = "simplytek.lk"
        
        # Store collections data for category mapping
        # self.collections_data = {}
        
    def fetch_all_products(self) -> Dict[str, Any]:
        """Fetch all products from the API"""
        try:
            response = requests.get(self.products_url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching products: {e}")
            return {"products": []}
        
    def get_product_categories(self, product_data: Dict) -> List[str]:
        """Extract category path from product data"""
        categories = []
        
        # Try to get categories from product_type
        product_type = product_data.get("product_type", "").strip()
        if product_type:
            categories.append(product_type)
        
        # Try to get categories from tags
        tags = product_data.get("tags", [])
        for tag in tags:
            if tag and isinstance(tag, str):
                tag = tag.strip()
                if tag and tag not in categories:
                    categories.append(tag)
        
        # If no categories found, try to infer from title or set default
        if not categories:
            title = product_data.get("title", "").lower()
            if "watch" in title or "strap" in title:
                categories.append("Accessories")
            elif "camera" in title:
                categories.append("Cameras")
            elif "phone" in title or "mobile" in title:
                categories.append("Mobile Phones")
            else:
                categories.append("General")
        
        return categories
    
    def build_product_url(self, product_handle: str) -> str:
        """Build the full product URL"""
        return f"{self.base_url}/products/{product_handle}"
    
    def format_price(self, price_value: Any) -> str:
        """Format price value to string"""
        if price_value is None:
            return "0"
        
        # Convert to float first, then format
        try:
            price_float = float(price_value)
            # Remove decimal if it's .00
            if price_float == int(price_float):
                return str(int(price_float))
            else:
                return f"{price_float:.2f}"
        except (ValueError, TypeError):
            return "0"
     
    def extract_product_data(self, product_data: Dict) -> Dict[str, Any]:
        """Extract product data according to the specified schema"""
        
        # Basic product information
        product_id_native = str(product_data.get("id", ""))
        product_handle = product_data.get("handle", "")
        product_url = self.build_product_url(product_handle)
        product_title = product_data.get("title", "")
        description_html = product_data.get("body_html")
        brand = product_data.get("vendor")  # In Shopify, vendor is typically the brand
        
        # Category path
        category_path = self.get_product_categories(product_data)
        
        # Image URLs
        images = product_data.get("images", [])
        image_urls = [img.get("src") for img in images if img.get("src")]
        
        # Extract variants
        variants = []
        for variant_data in product_data.get("variants", []):
            variant = {
                "variant_id_native": str(variant_data.get("id", "")),
                "variant_title": variant_data.get("title", "Default"),
                "price_current": self.format_price(variant_data.get("price")),
                "price_original": self.format_price(variant_data.get("compare_at_price")) if variant_data.get("compare_at_price") else None,
                "currency": "LKR",  # Sri Lankan Rupee
                "availability_text": "In Stock" if variant_data.get("available") else "Out of Stock"
            }
            variants.append(variant)
        
        # Ensure at least one variant exists
        if not variants:
            variants.append({
                "variant_id_native": product_id_native,
                "variant_title": "Default",
                "price_current": "0",
                "price_original": None,
                "currency": "LKR",
                "availability_text": "Out of Stock"
            })
        
        # Build the complete product object
        product_object = {
            "product_id_native": product_id_native,
            "product_url": product_url,
            "product_title": product_title,
            "description_html": description_html,
            "brand": brand,
            "category_path": category_path,
            "image_urls": image_urls,
            "variants": variants,
            "metadata": {
                "source_website": self.source_website,
                "shop_contact_phone": self.shop_phone,
                "shop_contact_whatsapp": self.shop_whatsapp,
                "scrape_timestamp": self.scrape_timestamp
            }
        }
        
        return product_object
    
    def extract_all_products(self) -> Dict[str, Any]:
        """Main method to extract all products according to schema"""
        
        # print("Fetching collections data...")
        # self.fetch_collections()
        
        print("Fetching all products...")
        products_data = self.fetch_all_products()
        products = products_data.get("products", [])
        
        print(f"Processing {len(products)} products...")
        
        extracted_products = []
        
        for i, product in enumerate(products):
            try:
                print(f"Processing product {i+1}/{len(products)}: {product.get('title', 'Unknown')}")
                
                extracted_product = self.extract_product_data(product)
                extracted_products.append(extracted_product)
                
            except Exception as e:
                print(f"Error processing product {i+1}: {e}")
                continue
        
        # Create the final output structure
        output_data = {
            "extraction_info": {
                "total_products_found": len(products),
                "total_products_extracted": len(extracted_products),
                "extraction_timestamp": self.scrape_timestamp,
                "source_website": self.source_website
            },
            "products": extracted_products
        }
        
        return output_data
    
    def save_extracted_data(self, data: Dict[str, Any], filename: str = "simplytek_products_extracted.json"):
        """Save extracted data to JSON file"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Data successfully saved to {filename}")
        except Exception as e:
            print(f"Error saving data: {e}")
    

# Usage example
if __name__ == "__main__":
    # Initialize the extractor
    extractor = ProductDataExtractor("all")
    
    # Process all products
    all_data = extractor.extract_all_products()
    print(f"Total products extracted: {len(all_data.get('products', []))}")
    print(all_data)

    extractor.save_extracted_data(all_data, "simplytek_products_extracted.json")

    
    