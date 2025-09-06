#!/usr/bin/env python3
"""
BigQuery loader for staging raw data from ADLS
"""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from google.cloud import bigquery
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from staging_schemas import (
    STAGING_SCHEMA, 
    get_staging_table_name, 
    get_staging_table_id,
    create_staging_table_ddl,
    validate_staging_table_schema,
    get_data_validation_query,
    get_sample_products_query
)

# Load environment variables from project root
import sys
from pathlib import Path

# Get project root (4 levels up from this file)
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BigQueryLoader:
    def __init__(self, project_id: str = None, staging_dataset: str = None):
        """Initialize BigQuery loader with ADLS integration"""
        # Use default credentials (from gcloud auth) for better compatibility
        old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if old_creds:
            logger.info("🔄 Using gcloud default credentials instead of service account file")
            os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
        
        self.project_id = project_id or os.getenv("BIGQUERY_PROJECT_ID", "price-pulse-470211")
        self.staging_dataset = staging_dataset or os.getenv("BIGQUERY_STAGING_DATASET", "staging")
        
        # Initialize BigQuery client
        try:
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"✅ BigQuery client initialized for {self.project_id}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize BigQuery client: {e}")
            logger.info("💡 Make sure you've run: gcloud auth application-default login")
            raise
        
        # Initialize Azure client for direct ADLS access
        azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if azure_connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
            self.azure_container = os.getenv("AZURE_CONTAINER_NAME", "raw-data")
        else:
            self.blob_service_client = None
            logger.warning("⚠️ Azure connection not available - direct ADLS loading disabled")
        
        # Ensure staging dataset exists
        self._ensure_dataset_exists()
        
        logger.info(f"🚀 BigQuery Loader initialized for {self.project_id}.{self.staging_dataset}")
    
    def _ensure_dataset_exists(self):
        """Ensure staging dataset exists"""
        try:
            self.client.get_dataset(self.staging_dataset)
            logger.info(f" Dataset {self.staging_dataset} exists")
        except Exception as e:
            # Try to create dataset
            try:
                dataset_id = f"{self.project_id}.{self.staging_dataset}"
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"  # Adjust based on your region
                dataset.description = "Staging dataset for raw scraped data"
                
                dataset = self.client.create_dataset(dataset, timeout=30)
                logger.info(f" Created dataset {self.staging_dataset}")
            except Exception as create_error:
                if "Access Denied" in str(create_error) or "does not have bigquery.datasets.create permission" in str(create_error):
                    logger.warning(f"⚠️  Cannot create dataset {self.staging_dataset}. Please ask admin to create it or grant permissions.")
                    logger.info(f"💡 Dataset creation command: CREATE SCHEMA `{self.project_id}.{self.staging_dataset}`")
                    # Continue anyway - maybe dataset exists but we can't see it
                else:
                    logger.error(f"❌ Failed to create dataset: {create_error}")
                    raise
    
    def ensure_staging_table_exists(self, source: str) -> str:
        """Ensure staging table exists for a source"""
        table_name = get_staging_table_name(source)
        table_id = get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            table = self.client.get_table(table_id)
            logger.debug(f" Table {table_name} exists")
            
            # Validate schema
            schema_issues = validate_staging_table_schema(table)
            if schema_issues:
                logger.warning(f" Schema issues in {table_name}: {schema_issues}")
            
        except Exception:
            # Create table
            table = bigquery.Table(table_id, schema=STAGING_SCHEMA)
            
            # Add partitioning by scrape_date
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="scrape_date"
            )
            
            # Add clustering by source_website
            table.clustering_fields = ["source_website"]
            
            # Set table description
            table.description = f"Raw scraped data staging table for {source}"
            
            # Set partition expiration (1 day - immediate overwrite for staging)
            table.time_partitioning.expiration_ms = 1 * 24 * 60 * 60 * 1000
            
            table = self.client.create_table(table)
            logger.info(f" Created table {table_name}")
        
        return table_id
    
    def load_source_data(self, 
                        source: str, 
                        products: List[Dict], 
                        scrape_date: str, 
                        file_path: str = None) -> int:
        """Load products from a source to staging table - OVERWRITE EXISTING DATA"""
        if not products:
            logger.warning(f"⚠️ No products to load for {source}")
            return 0
        
        # Ensure table exists
        table_id = self.ensure_staging_table_exists(source)
        
        # Prepare single row with entire product array
        load_timestamp = datetime.utcnow().isoformat()
        
        row_to_insert = {
            "raw_json_data": json.dumps(products),  # Convert Python list to JSON string
            "scrape_date": scrape_date,
            "source_website": source,
            "loaded_at": load_timestamp,
            "file_path": file_path,
            "product_count": len(products)
        }
        
        # Strategy for overwrite: 
        # 1. Try DELETE (works if no streaming buffer)
        # 2. If DELETE fails, use timestamp-based uniqueness 
        # 3. Rely on 1-day partition retention for cleanup
        
        delete_attempted = False
        try:
            # Attempt to clear existing data
            delete_query = f"""
            DELETE FROM `{table_id}` 
            WHERE source_website = '{source}'
            """
            
            delete_job = self.client.query(delete_query)
            delete_job.result()  # Wait for completion
            logger.info(f"🗑️ Cleared existing data for {source}")
            delete_attempted = True
            
        except Exception as e:
            if "streaming buffer" in str(e).lower():
                logger.info(f"📊 Using timestamp-based overwrite for {source} (streaming buffer active)")
                
                # Check if table has overwrite_timestamp field (new tables vs legacy tables)
                try:
                    table = self.client.get_table(table_id)
                    has_overwrite_field = any(field.name == "overwrite_timestamp" for field in table.schema)
                    
                    if has_overwrite_field:
                        # Add timestamp to make this row unique and most recent
                        row_to_insert["overwrite_timestamp"] = datetime.utcnow().timestamp()
                        logger.debug(f"Added overwrite_timestamp for {source}")
                    else:
                        logger.info(f"Legacy table {source} - using loaded_at for ordering")
                        
                except Exception as schema_e:
                    logger.warning(f"Could not check schema for {source}: {schema_e}")
                    
            else:
                logger.warning(f"⚠️ Could not clear existing data for {source}: {str(e)}")
        
        # Insert new row
        try:
            errors = self.client.insert_rows_json(table_id, [row_to_insert])
            
            if errors:
                logger.error(f"❌ Failed to insert data to {table_id}: {errors}")
                return 0
            else:
                mode_info = "OVERWRITE MODE" if delete_attempted else "APPEND MODE (will overwrite via 1-day retention)"
                logger.info(f"✅ Loaded {len(products)} products as 1 row to {get_staging_table_name(source)} ({mode_info})")
                return len(products)
                
        except Exception as e:
            logger.error(f"❌ Exception inserting data to {table_id}: {str(e)}")
            return 0

    def load_from_adls_blob(self, source: str, scrape_date: str) -> int:
        """Load data directly from ADLS blob to BigQuery staging - PRODUCTION METHOD"""
        if not self.blob_service_client:
            logger.error("❌ Azure connection not available for direct ADLS loading")
            return 0
        
        try:
            # Build ADLS blob path
            blob_path = f"source_website={source}/scrape_date={scrape_date}/data.json"
            
            # Download blob data
            blob_client = self.blob_service_client.get_blob_client(
                container=self.azure_container,
                blob=blob_path
            )
            
            logger.info(f"📥 Downloading {blob_path} from ADLS...")
            blob_data = blob_client.download_blob().readall()
            
            # Parse JSON data
            products = json.loads(blob_data.decode('utf-8'))
            
            # Ensure it's a list
            if not isinstance(products, list):
                products = [products]
            
            logger.info(f"📊 Found {len(products)} products in {blob_path}")
            
            # Load to BigQuery staging
            loaded_count = self.load_source_data(
                source=source,
                products=products,
                scrape_date=scrape_date,
                file_path=blob_path
            )
            
            if loaded_count > 0:
                logger.info(f"🎯 Successfully loaded {source}: {loaded_count} products")
            
            return loaded_count
            
        except Exception as e:
            logger.error(f"❌ Failed to load {source} from ADLS: {str(e)}")
            return 0

    def load_multiple_sources_from_adls(self, sources: List[str], scrape_date: str) -> Dict[str, int]:
        """Load multiple sources from ADLS in parallel"""
        results = {}
        total_products = 0
        successful_sources = 0
        
        logger.info(f"🚀 Starting ADLS → BigQuery loading for {len(sources)} sources on {scrape_date}")
        
        for source in sources:
            try:
                loaded_count = self.load_from_adls_blob(source, scrape_date)
                results[source] = loaded_count
                
                if loaded_count > 0:
                    successful_sources += 1
                    total_products += loaded_count
                else:
                    logger.warning(f"⚠️ No products loaded for {source}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to process {source}: {str(e)}")
                results[source] = 0
        
        # Summary
        logger.info(f"🎯 Loading Summary:")
        logger.info(f"   Date: {scrape_date}")
        logger.info(f"   Sources attempted: {len(sources)}")
        logger.info(f"   Sources successful: {successful_sources}")
        logger.info(f"   Total products loaded: {total_products}")
        logger.info(f"   BigQuery rows created: {successful_sources}")
        
        return results
    
    def validate_load(self, source: str, scrape_date: str) -> Dict:
        """Validate data loaded for a source and date"""
        table_id = get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            # Run validation query
            query = get_data_validation_query(table_id, scrape_date)
            result = self.client.query(query).result()
            
            validation_results = []
            for row in result:
                validation_result = {
                    'source': row.source_website,
                    'scrape_date': str(row.scrape_date),
                    'row_count': row.row_count,
                    'total_products': row.total_products,
                    'avg_products_per_source': row.avg_products_per_source,
                    'first_loaded': row.first_loaded,
                    'last_loaded': row.last_loaded,
                    'file_paths': row.file_paths
                }
                validation_results.append(validation_result)
            
            if validation_results:
                result = validation_results[0]  # Should only be one result
                logger.info(f"✅ {source} validation: {result['total_products']} products in {result['row_count']} row(s)")
                return result
            else:
                logger.warning(f"⚠️ No data found for {source} on {scrape_date}")
                return {'source': source, 'scrape_date': scrape_date, 'row_count': 0}
                
        except Exception as e:
            logger.error(f" Validation failed for {source}: {str(e)}")
            return {'source': source, 'scrape_date': scrape_date, 'error': str(e)}
    
    def get_sample_products(self, source: str, scrape_date: str, limit: int = 5) -> List[Dict]:
        """Get sample products for inspection"""
        table_id = get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            query = get_sample_products_query(table_id, scrape_date, limit)
            result = self.client.query(query).result()
            
            samples = []
            for row in result:
                sample = {
                    'product_id': row.product_id,
                    'title': row.title,
                    'brand': row.brand,
                    'source': row.source_website,
                    'variant_count': row.variant_count,
                    'loaded_at': row.loaded_at
                }
                samples.append(sample)
            
            logger.info(f" Retrieved {len(samples)} sample products from {source}")
            return samples
            
        except Exception as e:
            logger.error(f" Failed to get samples for {source}: {str(e)}")
            return []
    
    def load_multiple_sources(self, 
                             all_source_data: Dict[str, List[Dict]], 
                             scrape_date: str) -> Dict[str, int]:
        """Load data from multiple sources"""
        load_results = {}
        total_loaded = 0
        
        logger.info(f" Loading data from {len(all_source_data)} sources for {scrape_date}")
        
        for source, products in all_source_data.items():
            try:
                if products:
                    rows_loaded = self.load_source_data(source, products, scrape_date)
                    load_results[source] = rows_loaded
                    total_loaded += rows_loaded
                    
                    if rows_loaded > 0:
                        # Validate the load
                        validation = self.validate_load(source, scrape_date)
                        if validation.get('row_count', 0) != rows_loaded:
                            logger.warning(f"⚠️ {source}: Loaded {rows_loaded} but found {validation.get('row_count', 0)} in table")
                else:
                    load_results[source] = 0
                    logger.warning(f"⚠️ {source}: No products to load")
                    
            except Exception as e:
                logger.error(f" {source}: Failed to load - {str(e)}")
                load_results[source] = 0
        
        # Summary
        successful_sources = len([r for r in load_results.values() if r > 0])
        logger.info(f" Load Summary:")
        logger.info(f"   Sources attempted: {len(all_source_data)}")
        logger.info(f"   Sources successful: {successful_sources}")
        logger.info(f"   Total rows loaded: {total_loaded}")
        
        return load_results
    
    def get_staging_table_stats(self, source: str, days: int = 7) -> Dict:
        """Get statistics for a staging table over recent days"""
        table_id = get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        query = f"""
        SELECT 
            scrape_date,
            COUNT(*) as row_count,
            COUNT(DISTINCT JSON_EXTRACT_SCALAR(raw_data, '$.product_id_native')) as unique_products,
            AVG(product_count) as avg_file_size
        FROM `{table_id}`
        WHERE scrape_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY scrape_date
        ORDER BY scrape_date DESC
        """
        
        try:
            result = self.client.query(query).result()
            
            stats = []
            for row in result:
                stats.append({
                    'scrape_date': str(row.scrape_date),
                    'row_count': row.row_count,
                    'unique_products': row.unique_products,
                    'avg_file_size': round(row.avg_file_size) if row.avg_file_size else 0
                })
            
            return {'source': source, 'stats': stats}
            
        except Exception as e:
            logger.error(f" Failed to get stats for {source}: {str(e)}")
            return {'source': source, 'error': str(e)}

def main():
    """Test the BigQuery loader with direct ADLS loading"""
    logger.info("🧪 Testing BigQuery Loader with ADLS Integration")
    
    # Initialize loader
    loader = BigQueryLoader()
    
    # Test sources and date (matching our successful extraction)
    test_sources = ["appleme", "simplytek", "onei.lk"]
    test_date = "2025-09-03"
    
    logger.info(f"🎯 Testing direct ADLS → BigQuery loading for {test_date}")
    
    # Load data from ADLS to BigQuery
    results = loader.load_multiple_sources_from_adls(test_sources, test_date)
    
    # Validate each load
    for source in test_sources:
        if results.get(source, 0) > 0:
            logger.info(f"🔍 Validating {source}...")
            validation = loader.validate_load(source, test_date)
            
            # Get sample products
            samples = loader.get_sample_products(source, test_date, limit=3)
            if samples:
                logger.info(f"📝 Sample products from {source}:")
                for sample in samples:
                    logger.info(f"   • {sample['title']} ({sample['brand']})")
        else:
            logger.warning(f"⚠️ No data loaded for {source}")
    
    logger.info("🎉 BigQuery loading test completed!")

if __name__ == "__main__":
    main()