"""
Product schema models using Pydantic for validation
"""
from pydantic import BaseModel, HttpUrl, validator
from typing import List, Optional
from datetime import datetime


class VariantModel(BaseModel):
    """Product variant model"""
    variant_id_native: str
    variant_title: str
    price_current: str
    price_original: Optional[str] = None
    currency: str
    availability_text: str


class MetadataModel(BaseModel):
    """Product metadata model"""
    source_website: str
    shop_contact_phone: Optional[str] = None
    shop_contact_whatsapp: Optional[str] = None
    scrape_timestamp: datetime


class ProductModel(BaseModel):
    """Main product model"""
    product_id_native: str
    product_url: str
    product_title: str
    description_html: Optional[str] = None
    brand: Optional[str] = None
    category_path: List[str]
    image_urls: List[str]
    variants: List[VariantModel]
    metadata: MetadataModel

    @validator('image_urls')
    def validate_image_urls(cls, v):
        """Validate that image URLs are not empty"""
        return [url for url in v if url.strip()]

    @validator('category_path')
    def validate_category_path(cls, v):
        """Validate that category path is not empty"""
        return [cat for cat in v if cat.strip()]


class ScrapingResultModel(BaseModel):
    """Scraping session result model"""
    total_products: int
    successful_scrapes: int
    failed_scrapes: int
    categories_processed: int
    start_time: datetime
    end_time: datetime
    products: List[ProductModel]