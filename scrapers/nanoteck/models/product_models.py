"""
Pydantic models for Nanotek data validation and schema enforcement
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime


class ProductVariant(BaseModel):
    """Model for product variants"""
    variant_id_native: str = Field(..., description="Native variant ID from the source")
    variant_title: str = Field(..., description="Variant title/name")
    price_current: float = Field(..., description="Current price as float")
    price_original: Optional[float] = Field(None, description="Original price if different from current")
    currency: str = Field(default="LKR", description="Currency code")
    availability_text: str = Field(..., description="Availability status text")
    
    @validator('variant_id_native')
    def validate_variant_id(cls, v):
        if not v or not str(v).strip():
            raise ValueError('variant_id_native cannot be empty')
        return str(v).strip()
    
    @validator('price_current')
    def validate_price_current(cls, v):
        if v < 0:
            raise ValueError('price_current cannot be negative')
        return v


class ProductMetadata(BaseModel):
    """Model for product metadata"""
    source_website: str = Field(..., description="Source website URL")
    shop_contact_phone: Optional[str] = Field(None, description="Shop contact phone number")
    shop_contact_whatsapp: Optional[str] = Field(None, description="Shop WhatsApp contact")
    scrape_timestamp: datetime = Field(default_factory=datetime.now, description="Timestamp when data was scraped")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Product(BaseModel):
    """Main product model for Nanotek"""
    product_id_native: str = Field(..., description="Native product ID from the source")
    product_url: str = Field(..., description="Product page URL")
    product_title: str = Field(..., description="Product title/name")
    description_html: Optional[str] = Field(None, description="Product description in HTML format")
    brand: Optional[str] = Field(None, description="Product brand")
    category_path: List[str] = Field(default_factory=list, description="Category hierarchy path")
    specifications: Dict[str, str] = Field(default_factory=dict, description="Product specifications")
    image_urls: List[str] = Field(default_factory=list, description="List of product image URLs")
    variants: List[ProductVariant] = Field(..., description="List of product variants")
    metadata: ProductMetadata = Field(..., description="Metadata about the scraping")
    
    @validator('product_id_native')
    def validate_product_id(cls, v):
        if not v or not str(v).strip():
            raise ValueError('product_id_native cannot be empty')
        return str(v).strip()
    
    @validator('product_title')
    def validate_product_title(cls, v):
        if not v or not str(v).strip():
            raise ValueError('product_title cannot be empty')
        return str(v).strip()
    
    @validator('variants')
    def validate_variants(cls, v):
        if not v or len(v) == 0:
            raise ValueError('Product must have at least one variant')
        return v


class ScrapingResult(BaseModel):
    """Model for the complete scraping result"""
    total_products: int = Field(..., description="Total number of products scraped")
    total_variants: int = Field(..., description="Total number of variants scraped")
    categories_scraped: List[str] = Field(default_factory=list, description="List of categories scraped")
    scraping_metadata: Dict[str, Any] = Field(default_factory=dict, description="Scraping session metadata")
    products: List[Product] = Field(default_factory=list, description="List of scraped products")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ScrapingStats(BaseModel):
    """Model for scraping statistics"""
    start_time: datetime
    end_time: Optional[datetime] = None
    total_urls_discovered: int = 0
    products_scraped: int = 0
    failed_scrapes: int = 0
    success_rate: float = 0.0
    duration_seconds: float = 0.0