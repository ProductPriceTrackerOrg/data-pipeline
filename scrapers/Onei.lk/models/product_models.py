"""
Pydantic models for Onei.lk product data validation and schema enforcement
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict
from datetime import datetime, timezone

class ProductVariant(BaseModel):
    variant_id_native: str = Field(..., description="Native variant ID from the source")
    variant_title: str = Field(..., description="Variant title/name")
    price_current: str = Field(..., description="Current price as string")
    price_original: Optional[str] = Field(None, description="Original price if different from current")
    currency: str = Field(default="LKR", description="Currency code")
    availability_text: str = Field(..., description="Availability status text")

    @validator('variant_id_native', 'variant_title', 'price_current')
    def not_empty(cls, v):
        if not v or not str(v).strip():
            raise ValueError('Field cannot be empty')
        return str(v).strip()

def utc_now():
    return datetime.now(timezone.utc)

class ProductMetadata(BaseModel):
    source_website: str = Field(..., description="Source website URL")
    shop_contact_phone: Optional[str] = Field(None, description="Shop contact phone number")
    shop_contact_whatsapp: Optional[str] = Field(None, description="Shop WhatsApp contact")
    scrape_timestamp: datetime = Field(default_factory=utc_now)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class Product(BaseModel):
    product_id_native: str = Field(..., description="Native product ID from the source")
    product_url: str = Field(..., description="Product page URL")
    product_title: str = Field(..., description="Product title/name")
    description_html: Optional[str] = Field(None, description="Product description in HTML format")
    brand: Optional[str] = Field(None, description="Product brand")
    category_path: List[str] = Field(default_factory=list, description="Category hierarchy path")
    image_urls: List[str] = Field(default_factory=list, description="List of product image URLs")
    variants: List[ProductVariant] = Field(..., description="List of product variants")
    metadata: ProductMetadata = Field(..., description="Metadata about the scraping")

    @validator('product_id_native', 'product_title', 'product_url')
    def not_empty(cls, v):
        if not v or not str(v).strip():
            raise ValueError('Field cannot be empty')
        return str(v).strip()

    @validator('variants')
    def validate_variants(cls, v):
        if not v or len(v) == 0:
            raise ValueError('Product must have at least one variant')
        return v

    @validator('image_urls')
    def validate_image_urls(cls, v):
        return [url for url in v if url and str(url).strip()]

class ScrapingResult(BaseModel):
    total_products: int = Field(..., description="Total number of products scraped")
    total_variants: int = Field(..., description="Total number of variants scraped")
    categories_scraped: List[str] = Field(default_factory=list, description="List of categories scraped")
    scraping_metadata: Dict[str, str] = Field(default_factory=dict, description="Scraping session metadata")
    products: List[Product] = Field(default_factory=list, description="List of scraped products")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }