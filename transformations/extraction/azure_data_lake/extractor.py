#!/usr/bin/env python3
"""
Adaptive ADLS extractor for raw product data
Automatically discovers new sources and extracts data
"""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from azure.storage.blob import BlobServiceClient
from config import (
    AZURE_STORAGE_CONNECTION_STRING, 
    AZURE_CONTAINER_NAME, 
    EXPECTED_SOURCES,
    MIN_PRODUCTS_PER_SOURCE,
    validate_config
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ADLSExtractor:
    def __init__(self):
        """Initialize ADLS extractor with validation"""
        validate_config()
        
        self.blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        self.container_name = AZURE_CONTAINER_NAME
        
    def discover_sources(self) -> List[str]:
        """Automatically discover all available sources in ADLS"""
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            sources = set()
            
            for blob in container_client.list_blobs():
                # Look for source_website= pattern in path
                if 'source_website=' in blob.name:
                    path_parts = blob.name.split('/')
                    for part in path_parts:
                        if part.startswith('source_website='):
                            source_name = part.replace('source_website=', '')
                            sources.add(source_name)
            
            sources_list = sorted(list(sources))
            logger.info(f"🔍 Discovered {len(sources_list)} sources: {sources_list}")
            
            # Warn about unexpected sources
            unexpected_sources = set(sources_list) - set(EXPECTED_SOURCES)
            if unexpected_sources:
                logger.info(f"🆕 New sources detected: {unexpected_sources}")
            
            return sources_list
            
        except Exception as e:
            logger.error(f"❌ Source discovery failed: {str(e)}")
            return []
    
    def get_latest_file_for_source(self, source: str, date: str) -> Optional[str]:
        """Get the latest file for a source on a specific date"""
        try:
            # Build search path based on ADLS structure
            search_path = f"source_website={source}/scrape_date={date}/"
            
            container_client = self.blob_service_client.get_container_client(self.container_name)
            
            matching_files = []
            for blob in container_client.list_blobs(name_starts_with=search_path):
                if blob.name.endswith('.json'):
                    matching_files.append((blob.name, blob.last_modified))
            
            if matching_files:
                # Return the most recent file
                latest_file = max(matching_files, key=lambda x: x[1])
                logger.debug(f"📁 {source}: Found {latest_file[0]}")
                return latest_file[0]
            else:
                logger.warning(f"⚠️ {source}: No files found for {date} in {search_path}")
                return None
                
        except Exception as e:
            logger.error(f"❌ {source}: Error finding file for {date} - {str(e)}")
            return None
    
    def download_file(self, blob_name: str) -> List[Dict]:
        """Download and parse a JSON file from ADLS"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )
            
            # Download and parse
            blob_data = blob_client.download_blob().readall()
            json_data = json.loads(blob_data.decode('utf-8'))
            
            # Ensure it's a list
            if isinstance(json_data, list):
                return json_data
            else:
                return [json_data]  # Wrap single object in list
                
        except Exception as e:
            logger.error(f"❌ Failed to download {blob_name}: {str(e)}")
            return []
    
    def validate_source_data(self, source: str, products: List[Dict]) -> Dict[str, any]:
        """Validate extracted data for a source"""
        validation = {
            'source': source,
            'product_count': len(products),
            'has_products': len(products) > 0,
            'meets_minimum': len(products) >= MIN_PRODUCTS_PER_SOURCE,
            'sample_fields': [],
            'issues': []
        }
        
        if products:
            # Check first product structure
            first_product = products[0]
            validation['sample_fields'] = list(first_product.keys())
            
            # Basic validation checks
            required_fields = ['product_id_native', 'product_title']
            for field in required_fields:
                if field not in first_product:
                    validation['issues'].append(f"Missing required field: {field}")
            
            # Check if all products have similar structure
            if len(products) > 1:
                field_consistency = all(
                    set(product.keys()) == set(first_product.keys()) 
                    for product in products[:5]  # Check first 5
                )
                if not field_consistency:
                    validation['issues'].append("Inconsistent field structure across products")
            
            # Check minimum product count
            if not validation['meets_minimum']:
                validation['issues'].append(f"Product count ({len(products)}) below minimum ({MIN_PRODUCTS_PER_SOURCE})")
        
        return validation
    
    def extract_for_date(self, date: str, sources: Optional[List[str]] = None) -> Dict[str, List[Dict]]:
        """Extract data from all or specified sources for a specific date"""
        logger.info(f"🚀 Starting extraction for {date}")
        
        # Use provided sources or discover all
        if sources is None:
            sources = self.discover_sources()
        
        if not sources:
            logger.warning("⚠️ No sources to extract from")
            return {}
        
        results = {}
        total_products = 0
        successful_sources = 0
        
        for source in sources:
            try:
                # Get latest file for this source
                latest_file = self.get_latest_file_for_source(source, date)
                
                if latest_file:
                    # Download and parse
                    products = self.download_file(latest_file)
                    results[source] = products
                    total_products += len(products)
                    
                    # Validate data
                    validation = self.validate_source_data(source, products)
                    
                    if validation['issues']:
                        logger.warning(f"⚠️ {source}: Validation issues - {validation['issues']}")
                    
                    if validation['has_products']:
                        successful_sources += 1
                        logger.info(f"✅ {source}: {len(products)} products")
                    else:
                        logger.warning(f"⚠️ {source}: No products extracted")
                else:
                    results[source] = []
                    logger.warning(f"⚠️ {source}: No data found for {date}")
                    
            except Exception as e:
                logger.error(f"❌ {source}: Extraction failed - {str(e)}")
                results[source] = []
        
        # Summary
        logger.info(f"🎯 Extraction Summary:")
        logger.info(f"   Date: {date}")
        logger.info(f"   Sources attempted: {len(sources)}")
        logger.info(f"   Sources successful: {successful_sources}")
        logger.info(f"   Total products: {total_products}")
        
        return results
    
    def save_extraction_results(self, data: Dict[str, List[Dict]], date: str) -> str:
        """Save extraction results to temp_data with timestamp for inspection"""
        try:
            # Create temp_data directory
            temp_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "temp_data")
            os.makedirs(temp_dir, exist_ok=True)
            
            # Save results with timestamp
            timestamp = datetime.utcnow().strftime("%H%M%S")
            output_file = os.path.join(temp_dir, f"extracted_data_{date}_{timestamp}.json")
            
            # Create summary for saving
            save_data = {
                'extraction_date': date,
                'extraction_timestamp': datetime.utcnow().isoformat(),
                'sources_count': len(data),
                'total_products': sum(len(products) for products in data.values()),
                'data': data
            }
            
            with open(output_file, 'w') as f:
                json.dump(save_data, f, indent=2, default=str)
            
            logger.info(f"💾 Results saved to {output_file} (extraction at {timestamp})")
            return output_file
            
        except Exception as e:
            logger.error(f"❌ Failed to save results: {str(e)}")
            return ""

def main():
    """Test the extractor with a sample date"""
    extractor = ADLSExtractor()
    
    # Test with a specific date (using date from our ADLS discovery)
    test_date = "2025-09-03"  # Date that exists in our ADLS
    
    logger.info(f"Testing extraction for {test_date}")
    
    # Extract data
    data = extractor.extract_for_date(test_date)
    
    if data:
        # Save results for inspection
        output_file = extractor.save_extraction_results(data, test_date)
        
        # Print summary
        for source, products in data.items():
            if products:
                logger.info(f"{source}: {len(products)} products extracted")
            else:
                logger.warning(f"{source}: No products extracted")
    else:
        logger.error("No data extracted from any source")

if __name__ == "__main__":
    main()