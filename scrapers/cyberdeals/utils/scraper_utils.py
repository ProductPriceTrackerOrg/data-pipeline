"""
Utility functions for the CyberDeals scraper
"""
import json
import os
import re
from typing import List, Dict, Any, Optional
from selectolax.parser import HTMLParser

def ensure_output_dir(directory: str) -> None:
    """Ensure the output directory exists"""
    os.makedirs(directory, exist_ok=True)

def save_json(data: List[Dict[str, Any]], filepath: str) -> None:
    """Save data to a JSON file"""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Saved {len(data)} products to {filepath}")

def extract_text_from_price(price_node) -> str:
    """Extract numeric value from price node"""
    if not price_node:
        return "0"
    return re.sub(r"[^\d.]", "", price_node.text(strip=True))

def detect_brand(title: str, categories: List[str], url: str, known_brands: List[str]) -> Optional[str]:
    """Detect brand from product information using multiple strategies"""
    brand = None
    
    # First check if any category directly contains a known brand
    for category in categories:
        for known_brand in known_brands:
            if known_brand.lower() in category.lower():
                brand = known_brand.title()
                break
        if brand:
            break
    
    # If no brand found, try extracting from title
    if not brand and title:
        for known_brand in known_brands:
            if known_brand.lower() in title.lower():
                brand = known_brand.title()
                break
    
    # If still no brand, try extracting from URL
    if not brand and url:
        for known_brand in known_brands:
            if known_brand.lower() in url.lower():
                brand = known_brand.title()
                break
                
    # Special case for TP-Link which might be formatted differently
    if brand and brand.lower() == "tp-link":
        brand = "TP-Link"
        
    return brand

def extract_image_urls(tree: HTMLParser) -> List[str]:
    """Extract product image URLs with fallback strategies"""
    image_urls = []
    
    # First try figure elements
    figure_images = [
        node.attributes.get("href") for node in tree.css("figure.woocommerce-product-gallery__image a")
        if node.attributes.get("href")
    ]
    image_urls.extend(figure_images)
    
    # Then try div elements if no images found yet
    if not image_urls:
        div_images = [
            node.attributes.get("href") for node in tree.css("div.woocommerce-product-gallery__image a")
            if node.attributes.get("href")
        ]
        image_urls.extend(div_images)
    
    # Try fallback for any gallery images
    if not image_urls:
        fallback_images = [
            node.attributes.get("href") for node in tree.css(".woocommerce-product-gallery a")
            if node.attributes.get("href") and (
                node.attributes.get("href").endswith('.jpg') or 
                node.attributes.get("href").endswith('.jpeg') or 
                node.attributes.get("href").endswith('.png')
            )
        ]
        image_urls.extend(fallback_images)
        
    # Try getting the main product image if nothing else worked
    if not image_urls:
        main_image = tree.css_first(".wp-post-image")
        if main_image and "src" in main_image.attributes:
            image_urls.append(main_image.attributes["src"])
    
    # Remove duplicates while preserving order
    return list(dict.fromkeys(image_urls))
