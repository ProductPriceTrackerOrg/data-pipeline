from pydantic import BaseModel, HttpUrl, validator
from typing import List, Optional, Union
from datetime import datetime


class Variant(BaseModel):
    variant_id_native: str
    variant_title: str
    price_current: str
    price_original: Optional[str] = None
    currency: str = "LKR"
    availability_text: str

    @validator('price_current', 'price_original')
    def validate_price(cls, v):
        if v is not None and not v.startswith('Rs'):
            raise ValueError('Price must start with Rs')
        return v


class Metadata(BaseModel):
    source_website: str = "simplytek.lk"
    shop_contact_phone: str = "0117555888"
    shop_contact_whatsapp: str = "+94 72 672 9729"
    scrape_timestamp: datetime

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class Product(BaseModel):
    product_id_native: str
    product_url: str
    product_title: str
    description_html: Optional[str] = None
    brand: Optional[str] = None
    category_path: List[str]
    image_urls: List[str]
    variants: List[Variant]
    metadata: Metadata

    @validator('image_urls')
    def validate_image_urls(cls, v):
        if not v:
            raise ValueError('At least one image URL is required')
        return v

    @validator('variants')
    def validate_variants(cls, v):
        if not v:
            raise ValueError('At least one variant is required')
        return v


class ScrapingResult(BaseModel):
    success: bool
    products: List[Product]
    total_products: int
    pages_scraped: int
    errors: List[str]
    execution_time: float