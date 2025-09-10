#!/usr/bin/env python3
"""
BigQuery loader for staging raw data from ADLS for a SPECIFIED DATE.
This script dynamically discovers sources but loads data only for the date
defined in the `DATE_TO_PROCESS` variable in the main function.
"""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict
from google.cloud import bigquery
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

import staging_schema

# Load environment variables from project root
from pathlib import Path

# Get project root (4 levels up from this file)
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BigQueryLoader:
    def __init__(self, project_id: str = None, staging_dataset: str = None):
        """Initialize BigQuery loader with ADLS integration"""
        old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if old_creds:
            logger.info("Using gcloud default credentials instead of service account file")
            os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
        
        self.project_id = project_id or os.getenv("BIGQUERY_PROJECT_ID", "price-pulse-470211")
        self.staging_dataset = staging_dataset or os.getenv("BIGQUERY_STAGING_DATASET", "staging")
        
        try:
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"BigQuery client initialized for {self.project_id}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize BigQuery client: {e}")
            logger.info("Make sure you've run: gcloud auth application-default login")
            raise
        
        azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if azure_connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
            self.azure_container = os.getenv("AZURE_CONTAINER_NAME", "raw-data")
        else:
            self.blob_service_client = None
            logger.warning("Azure connection not available - direct ADLS loading disabled")
        
        self._ensure_dataset_exists()
        
        logger.info(f"BigQuery Loader initialized for {self.project_id}.{self.staging_dataset}")
    
    def _ensure_dataset_exists(self):
        """Ensure staging dataset exists"""
        try:
            self.client.get_dataset(self.staging_dataset)
            logger.info(f" Dataset {self.staging_dataset} exists")
        except Exception as e:
            try:
                dataset_id = f"{self.project_id}.{self.staging_dataset}"
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = "US"
                dataset.description = "Staging dataset for raw scraped data"
                
                dataset = self.client.create_dataset(dataset, timeout=30)
                logger.info(f" Created dataset {self.staging_dataset}")
            except Exception as create_error:
                if "Access Denied" in str(create_error) or "does not have bigquery.datasets.create permission" in str(create_error):
                    logger.warning(f"Cannot create dataset {self.staging_dataset}. Please ask admin to create it or grant permissions.")
                    logger.info(f"Dataset creation command: CREATE SCHEMA `{self.project_id}.{self.staging_dataset}`")
                else:
                    logger.error(f"❌ Failed to create dataset: {create_error}")
                    raise
    
    def ensure_staging_table_exists(self, source: str) -> str:
        """Ensure staging table exists for a source"""
        table_name = staging_schema.get_staging_table_name(source)
        table_id = staging_schema.get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            table = self.client.get_table(table_id)
            logger.debug(f" Table {table_name} exists")
            
            schema_issues = staging_schema.validate_staging_table_schema(table)
            if schema_issues:
                logger.warning(f" Schema issues in {table_name}: {schema_issues}")
            
        except Exception:
            table = bigquery.Table(table_id, schema=staging_schema.STAGING_SCHEMA)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="scrape_date"
            )
            table.clustering_fields = ["source_website"]
            table.description = f"Raw scraped data staging table for {source}"
            table.time_partitioning.expiration_ms = 365 * 24 * 60 * 60 * 1000
            
            table = self.client.create_table(table)
            logger.info(f" Created table {table_name}")
        
        return table_id
    
    def load_source_data(self, 
                         source: str, 
                         products: List[Dict], 
                         scrape_date: str, 
                         file_path: str = None) -> int:
        """
        Loads products for a specific source and date using a robust BigQuery Load Job.
        """
        if not products:
            logger.warning(f"No products to load for {source}")
            return 0
        
        table_id = self.ensure_staging_table_exists(source)
        
        partition_decorator = scrape_date.replace("-", "")
        table_id_with_partition = f"{table_id}${partition_decorator}"
        
        row_to_load = {
            "raw_json_data": json.dumps(products),
            "scrape_date": scrape_date,
            "source_website": source,
            "loaded_at": datetime.utcnow().isoformat(),
            "file_path": file_path,
            "product_count": len(products)
        }
        
        job_config = bigquery.LoadJobConfig(
            schema=staging_schema.STAGING_SCHEMA,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        try:
            job = self.client.load_table_from_json(
                [row_to_load],
                table_id_with_partition,
                job_config=job_config,
            )
            job.result()

            logger.info(f"Successfully loaded {len(products)} products to {table_id} for partition {scrape_date}")
            return len(products)

        except Exception as e:
            logger.error(f"❌ Failed to load data to {table_id} using Load Job: {e}")
            return 0

    def load_from_adls_blob(self, source: str, scrape_date: str) -> int:
        """Load data directly from a specific ADLS blob to BigQuery staging."""
        if not self.blob_service_client:
            logger.error("❌ Azure connection not available for direct ADLS loading")
            return 0
        
        try:
            blob_path = f"source_website={source}/scrape_date={scrape_date}/data.json"
            blob_client = self.blob_service_client.get_blob_client(
                container=self.azure_container,
                blob=blob_path
            )
            
            logger.info(f"Downloading {blob_path} from ADLS...")
            blob_data = blob_client.download_blob().readall()
            
            products = json.loads(blob_data.decode('utf-8'))
            
            if not isinstance(products, list):
                products = [products]
            
            logger.info(f"Found {len(products)} products in {blob_path}")
            
            return self.load_source_data(
                source=source,
                products=products,
                scrape_date=scrape_date,
                file_path=blob_path
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to load {source} for date {scrape_date} from ADLS: {e}")
            return 0
    
    def validate_load(self, source: str, scrape_date: str) -> Dict:
        """Validate data loaded for a source and date"""
        table_id = staging_schema.get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            query = staging_schema.get_data_validation_query(table_id, scrape_date)
            result = list(self.client.query(query).result())
            
            if result:
                row = result[0]
                validation_result = dict(row.items())
                logger.info(f"{source} validation on {scrape_date}: {validation_result.get('total_products')} products in {validation_result.get('row_count')} row(s)")
                return validation_result
            else:
                logger.warning(f"No data found for {source} on {scrape_date} during validation.")
                return {'source': source, 'scrape_date': scrape_date, 'row_count': 0}
                
        except Exception as e:
            logger.error(f" Validation failed for {source} on {scrape_date}: {e}")
            return {'source': source, 'scrape_date': scrape_date, 'error': str(e)}
    
    def get_sample_products(self, source: str, scrape_date: str, limit: int = 5) -> List[Dict]:
        """Get sample products for inspection"""
        table_id = staging_schema.get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            query = staging_schema.get_sample_products_query(table_id, scrape_date, limit)
            result = self.client.query(query).result()
            
            samples = [dict(row.items()) for row in result]
            logger.info(f" Retrieved {len(samples)} sample products from {source} for {scrape_date}")
            return samples
            
        except Exception as e:
            logger.error(f" Failed to get samples for {source}: {e}")
            return []

def main():
    """
    Dynamically discovers all sources from ADLS and loads their data for a SPECIFIED DATE.
    """
    # ==============================================================================
    # --- EDIT THIS VARIABLE to the date you want to load (format: YYYY-MM-DD) ---
    DATE_TO_PROCESS = "2025-09-08"
    # ==============================================================================
    
    logger.info(f"Starting Manual BigQuery Staging Load for a SPECIFIED DATE: {DATE_TO_PROCESS}")
    
    loader = BigQueryLoader()
    
    if not loader.blob_service_client:
        logger.error("❌ Cannot discover sources. Azure connection not available.")
        return

    logger.info(f"Discovering all available sources in ADLS container '{loader.azure_container}'...")
    sources_to_process = set()
    try:
        container_client = loader.blob_service_client.get_container_client(loader.azure_container)
        
        for blob in container_client.list_blobs():
            if blob.name.startswith("source_website="):
                try:
                    source_name = blob.name.split('/')[0].split('=')[1]
                    if source_name:
                        sources_to_process.add(source_name)
                except IndexError:
                    continue
        
        if not sources_to_process:
            logger.warning("No sources found in ADLS container. Nothing to load.")
            return

        sources_list = sorted(list(sources_to_process))
        logger.info(f"Discovered {len(sources_list)} total sources. Will attempt to load data for {DATE_TO_PROCESS} for each.")

    except Exception as e:
        logger.error(f"❌ Failed to discover sources in ADLS: {e}")
        return
    
    results = {}
    logger.info("\nStarting ADLS → BigQuery loading process...")
    for source in sources_list:
        logger.info(f"--- Processing source: '{source}' for specified date: '{DATE_TO_PROCESS}' ---")
        loaded_count = loader.load_from_adls_blob(source, DATE_TO_PROCESS)
        results[source] = loaded_count

        if loaded_count > 0:
            logger.info(f"Validating load for '{source}' on '{DATE_TO_PROCESS}'...")
            loader.validate_load(source, DATE_TO_PROCESS)
            
            samples = loader.get_sample_products(source, DATE_TO_PROCESS, limit=3)
            if samples:
                logger.info(f"Sample products:")
                for sample in samples:
                    logger.info(f"   • {sample.get('title')} ({sample.get('brand')})")
        else:
            logger.warning(f"No data was loaded for '{source}' on '{DATE_TO_PROCESS}'. This is expected if the file does not exist.")
    
    logger.info("\nData loading for specified date completed!")

if __name__ == "__main__":
    main()

