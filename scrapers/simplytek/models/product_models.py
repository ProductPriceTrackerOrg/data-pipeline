"""
Pydantic models for data validation and schema enforcement
"""
from pydantic import BaseModel, HttpUrl, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone


class ProductVariant(BaseModel):
    """Model for product variants"""
    variant_id_native: str = Field(..., description="Native variant ID from the source")
    variant_title: str = Field(..., description="Variant title/name")
    price_current: str = Field(..., description="Current price as string")
    price_original: Optional[str] = Field(None, description="Original price if different from current")
    currency: str = Field(default="LKR", description="Currency code")
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


class ProductMetadata(BaseModel):
    """Model for product metadata"""
    source_website: str = Field(..., description="Source website URL")
    shop_contact_phone: Optional[str] = Field(None, description="Shop contact phone number")
    shop_contact_whatsapp: Optional[str] = Field(None, description="Shop WhatsApp contact")
    scrape_timestamp: datetime = Field(default_factory=datetime.now(timezone.utc), description="Timestamp when data was scraped")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Product(BaseModel):
    """Main product model matching the required schema"""
    product_id_native: str = Field(..., description="Native product ID from the source")
    product_url: str = Field(..., description="Product page URL")
    product_title: str = Field(..., description="Product title/name")
    description_html: Optional[str] = Field(None, description="Product description in HTML format")
    brand: Optional[str] = Field(None, description="Product brand")
    category_path: List[str] = Field(default_factory=list, description="Category hierarchy path")
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


# Raw Shopify API models for data transformation
class ShopifyImage(BaseModel):
    """Shopify image model"""
    id: int
    src: str
    width: Optional[int] = None
    height: Optional[int] = None
    variant_ids: List[int] = Field(default_factory=list)


class ShopifyVariant(BaseModel):
    """Shopify variant model"""
    id: int
    title: str
    option1: Optional[str] = None
    option2: Optional[str] = None
    option3: Optional[str] = None
    price: str
    compare_at_price: Optional[str] = None
    available: bool
    product_id: int


class ShopifyProduct(BaseModel):
    """Shopify product model for API response"""
    id: int
    title: str
    handle: str
    body_html: Optional[str] = None
    vendor: Optional[str] = None
    product_type: Optional[str] = None
    variants: List[ShopifyVariant]
    images: List[ShopifyImage] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)


class ShopifyApiResponse(BaseModel):
    """Shopify API response model"""
    products: List[ShopifyProduct]