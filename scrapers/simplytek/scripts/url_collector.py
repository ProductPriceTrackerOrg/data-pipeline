import json
import requests
from datetime import datetime
from typing import Dict, List, Any
import os
import pandas as pd

import warnings
warnings.filterwarnings("ignore")

class CollectionExtractor:
    def __init__(self, base_url: str = "https://www.simplytek.lk"):
        self.base_url = base_url.rstrip('/')
        self.collections_url = f"{self.base_url}/collections.json"
        self.scrape_timestamp = datetime.now().isoformat()

    def fetch_all_collections(self) -> Dict[str, Any]:
        """Fetch all collections from the API"""
        try:
            response = requests.get(self.collections_url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching collections: {e}")
            return {"collections": []}
        
    def extract_collection_info(self) -> List[Dict[str, Any]]:
        """Extract collection information and generate product URLs"""
        
        print("Fetching all collections...")
        collections_data = self.fetch_all_collections()
        collections = collections_data.get("collections", [])
        
        collection_info = []
        
        for collection in collections:
            # Extract collection details
            collection_detail = {
                "collection_id": collection.get("id"),
                "title": collection.get("title"),
                "handle": collection.get("handle"),
                "description": collection.get("description"),
                "description_text": self.strip_html(collection.get("description", "")),
                "published_at": collection.get("published_at"),
                "updated_at": collection.get("updated_at"),
                "products_count": collection.get("products_count", 0),
                "image_url": collection.get("image", {}).get("src") if collection.get("image") else None,
                "image_alt": collection.get("image", {}).get("alt") if collection.get("image") else None,
                
                # Generate URLs for this collection
                "collection_url": f"{self.base_url}/collections/{collection.get('handle')}",
                "products_json_url": f"{self.base_url}/collections/{collection.get('handle')}/products.json",
                
                # Metadata
                "scraped_at": self.scrape_timestamp
            }
            
            collection_info.append(collection_detail)
        
        return collection_info
    
    def strip_html(self, html_text: str) -> str:
        """Remove HTML tags from a string"""
        import re
        clean = re.compile('<.*?>')
        return re.sub(clean, '', html_text)
    
    def get_url(self, collections_info: List[Dict[str, Any]]):
        """"Get collection url from json file""" 
        
        # convert to Pandas
        df_collections=pd.DataFrame(collections_info)
        # Extract Url and convert to list
        collection_urls = list(df_collections["collection_url"])
        return collection_urls
    
def main():
    # Initialize the extractor
    extractor = CollectionExtractor()
    
    # Extract collection information
    collections_info = extractor.extract_collection_info()
    
    # Get collection URLs
    collection_urls = extractor.get_url(collections_info)
    
    # Print the collection URLs
    for url in collection_urls:
        print(url)
    
    
# main file 
if __name__ == "__main__":
    main()