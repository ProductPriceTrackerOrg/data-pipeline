"""
Data models for CyberDeals products
"""
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, field

@dataclass
class ProductVariant:
    variant_id_native: str
    variant_title: str
    price_current: str
    price_original: Optional[str] = None
    currency: str = "LKR"
    availability_text: str = "In Stock"

@dataclass
class ProductMetadata:
    source_website: str
    shop_contact_phone: str
    shop_contact_whatsapp: str
    scrape_timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class Product:
    product_id_native: str
    product_url: str
    product_title: str
    description_html: Optional[str]
    brand: Optional[str]
    category_path: List[str]
    image_urls: List[str]
    variants: List[ProductVariant]
    metadata: ProductMetadata

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "product_id_native": self.product_id_native,
            "product_url": self.product_url,
            "product_title": self.product_title,
            "description_html": self.description_html,
            "brand": self.brand,
            "category_path": self.category_path,
            "image_urls": self.image_urls,
            "variants": [vars(variant) for variant in self.variants],
            "metadata": vars(self.metadata)
        }
