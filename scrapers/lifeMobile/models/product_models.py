"""
Pydantic models for LifeMobile data validation and schema enforcement
"""
from pydantic import BaseModel, HttpUrl, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone


class PaymentOption(BaseModel):
    """Model for payment options"""
    provider: str = Field(..., description="Payment provider name")
    price: str = Field(..., description="Price for this payment method")
    
    @validator('provider')
    def validate_provider(cls, v):
        if not v or not str(v).strip():
            raise ValueError('provider cannot be empty')
        return str(v).strip()


class ProductVariant(BaseModel):
    """Model for product variants"""
    variant_id_native: str = Field(..., description="Native variant ID from the source")
    variant_title: str = Field(..., description="Variant title/name")
    price_current: str = Field(..., description="Current price as string")
    price_original: Optional[str] = Field(None, description="Original price if different from current")
    currency: str = Field(default="Rs.", description="Currency code")
    availability_text: str = Field(..., description="Availability status text")
    
    @validator('variant_id_native')
    def validate_variant_id(cls, v):
        if not v or not str(v).strip():
            raise ValueError('variant_id_native cannot be empty')
        return str(v).strip()
    
    @validator('price_current')
    def validate_price_current(cls, v):
        if not v or not str(v).strip():
            raise ValueError('price_current cannot be empty')
        return str(v).strip()
    
    @validator('variant_title')
    def validate_variant_title(cls, v):
        if not v or not str(v).strip():
            raise ValueError('variant_title cannot be empty')
        return str(v).strip()

def utc_now():
    return datetime.now(timezone.utc)

class ProductMetadata(BaseModel):
    """Model for product metadata"""
    source_website: str = Field(..., description="Source website URL")
    shop_contact_phone: Optional[str] = Field(None, description="Shop contact phone number")
    shop_contact_whatsapp: Optional[str] = Field(None, description="Shop WhatsApp contact")
    scrape_timestamp: datetime = Field(default_factory=utc_now)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Product(BaseModel):
    """Main product model for LifeMobile"""
    product_id_native: str = Field(..., description="Native product ID from the source")
    product_url: str = Field(..., description="Product page URL")
    product_title: str = Field(..., description="Product title/name")
    description_html: Optional[str] = Field(None, description="Product description in HTML format")
    brand: Optional[str] = Field(None, description="Product brand")
    category_path: List[str] = Field(default_factory=list, description="Category hierarchy path")
    specifications: Dict[str, str] = Field(default_factory=dict, description="Product specifications")
    image_urls: List[str] = Field(default_factory=list, description="List of product image URLs")
    variants: List[ProductVariant] = Field(..., description="List of product variants")
    payment_options: List[PaymentOption] = Field(default_factory=list, description="Available payment options")
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
    
    @validator('product_url')
    def validate_product_url(cls, v):
        if not v or not str(v).strip():
            raise ValueError('product_url cannot be empty')
        return str(v).strip()
    
    @validator('variants')
    def validate_variants(cls, v):
        if not v or len(v) == 0:
            raise ValueError('Product must have at least one variant')
        return v
    
    @validator('image_urls')
    def validate_image_urls(cls, v):
        # Filter out empty or invalid URLs
        return [url for url in v if url and str(url).strip()]


class ScrapingResult(BaseModel):
    """Model for the complete scraping result"""
    total_products: int = Field(..., description="Total number of products scraped")
    total_variants: int = Field(..., description="Total number of variants scraped")
    total_errors: int = Field(default=0, description="Total number of errors encountered")
    categories_scraped: List[str] = Field(default_factory=list, description="List of categories scraped")
    scraping_metadata: Dict[str, Any] = Field(default_factory=dict, description="Scraping session metadata")
    products: List[Product] = Field(default_factory=list, description="List of scraped products")
    
    @validator('products')
    def validate_products(cls, v):
        return v
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class LifeMobileScrapeStats(BaseModel):
    """Model for scraping statistics"""
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    products_scraped: int = 0
    categories_found: int = 0
    product_urls_found: int = 0
    errors_encountered: int = 0
    scraping_rate: Optional[float] = None  # products per second
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
    
    def finish_scraping(self):
        """Mark scraping as finished and calculate final stats"""
        self.end_time = datetime.now(timezone.utc)
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        if self.duration_seconds > 0:
            self.scraping_rate = self.products_scraped / self.duration_seconds