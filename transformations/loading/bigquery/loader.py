#!/usr/bin/env python3
"""
BigQuery loader for staging raw data from ADLS.
This script loads data for the current UTC date for all discovered sources.
"""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict
from google.cloud import bigquery
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

from staging_schema import (
    STAGING_SCHEMA, 
    get_staging_table_name, 
    get_staging_table_id,
    create_staging_table_ddl,
    validate_staging_table_schema,
    get_data_validation_query,
    get_sample_products_query
)

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
        # Set the environment variable to point to the credentials file
        # First, try to find the credentials file path
        credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if not credentials_path:
            # Look for the credentials file in the project root
            project_root_path = Path(__file__).parent.parent.parent.parent
            possible_creds_path = project_root_path / "gcp-credentials.json"
            if possible_creds_path.exists():
                credentials_path = str(possible_creds_path)
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                logger.info(f"Using service account credentials from: {credentials_path}")
            else:
                logger.warning("No credentials file found at project root")
        
        self.project_id = project_id or os.getenv("BIGQUERY_PROJECT_ID", "price-pulse-470211")
        self.staging_dataset = staging_dataset or os.getenv("BIGQUERY_STAGING_DATASET", "staging")
        
        # Initialize BigQuery client
        try:
            self.client = bigquery.Client(project=self.project_id)
            logger.info(f"BigQuery client initialized for {self.project_id}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize BigQuery client: {e}")
            logger.info("Make sure you have valid GCP credentials in gcp-credentials.json")
            raise
        
        # Initialize Azure client for direct ADLS access
        azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if azure_connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
            self.azure_container = os.getenv("AZURE_CONTAINER_NAME", "raw-data")
        else:
            self.blob_service_client = None
            logger.warning("Azure connection not available - direct ADLS loading disabled")
        
        # Ensure staging dataset exists
        self._ensure_dataset_exists()
        
        logger.info(f"BigQuery Loader initialized for {self.project_id}.{self.staging_dataset}")
    
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
                    logger.warning(f"Cannot create dataset {self.staging_dataset}. Please ask admin to create it or grant permissions.")
                    logger.info(f"Dataset creation command: CREATE SCHEMA `{self.project_id}.{self.staging_dataset}`")
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
            
            # Set partition expiration (e.g., 365 days)
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
        This method will completely clean the entire staging table before loading new data.
        """
        if not products:
            logger.warning(f"No products to load for {source}")
            return 0
        
        table_id = self.ensure_staging_table_exists(source)
        
        # Clean the entire staging table first
        try:
            # Execute DELETE query to remove all data from the table
            delete_query = f"DELETE FROM `{table_id}` WHERE TRUE"
            logger.info(f"Cleaning entire staging table with query: {delete_query}")
            
            delete_job = self.client.query(delete_query)
            delete_job.result()  # Wait for the deletion to complete
            logger.info(f"Successfully cleaned all data from {table_id}")
        except Exception as e:
            logger.error(f"❌ Failed to clean staging table {table_id}: {e}")
            # Continue with the load even if cleaning fails
        
        # Prepare the single row to be loaded. The entire product list becomes one JSON field.
        row_to_load = {
            "raw_json_data": json.dumps(products),
            "scrape_date": scrape_date,
            "source_website": source,
            "loaded_at": datetime.utcnow().isoformat(),
            "file_path": file_path,
            "product_count": len(products)
        }
        
        # Configure the Load Job for robustness 
        job_config = bigquery.LoadJobConfig(
            schema=STAGING_SCHEMA,
            # This tells BigQuery to treat the `raw_json_data` string as a JSON type.
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            # We don't need WRITE_TRUNCATE anymore since we manually cleaned the table
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        try:
            logger.info(f"Loading data to {table_id} after cleaning all existing data")
            # We pass a list containing our single row dict.
            # load_table_from_json is the robust way to handle bulk loads.
            job = self.client.load_table_from_json(
                [row_to_load],
                table_id,
                job_config=job_config,
            )
            job.result()  # Wait for the job to complete.

            logger.info(f"Successfully loaded {len(products)} products to {table_id} for date {scrape_date}")
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
            # This error is now expected if a file doesn't exist for the current day
            if "BlobNotFound" in str(e):
                 logger.warning(f"No data file found for '{source}' on date '{scrape_date}'. This is normal if scraping hasn't completed.")
            else:
                logger.error(f"❌ Failed to load {source} for date {scrape_date} from ADLS: {e}")
            return 0
    
    def validate_load(self, source: str, scrape_date: str) -> Dict:
        """Validate data loaded for a source and date"""
        table_id = get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            query = get_data_validation_query(table_id, scrape_date)
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
        table_id = get_staging_table_id(self.project_id, self.staging_dataset, source)
        
        try:
            query = get_sample_products_query(table_id, scrape_date, limit)
            result = self.client.query(query).result()
            
            samples = [dict(row.items()) for row in result]
            logger.info(f" Retrieved {len(samples)} sample products from {source} for {scrape_date}")
            return samples
            
        except Exception as e:
            logger.error(f" Failed to get samples for {source}: {e}")
            return []

def main():
    """
    Loads data for the CURRENT UTC DATE for all discovered sources in ADLS and loads to BigQuery.
    """
    logger.info("Loading BigQuery Staging with data for the current UTC date from ADLS")
    
    loader = BigQueryLoader()

    # --- GET CURRENT UTC DATE ---
    target_date = datetime.utcnow().strftime("%Y-%m-%d")
    logger.info(f"Targeting data for current UTC date: {target_date}")
    
    # --- DISCOVER ALL AVAILABLE SOURCES ---
    if not loader.blob_service_client:
        logger.error("❌ Cannot discover sources. Azure connection not available.")
        return

    logger.info(f"Discovering all available sources in ADLS container '{loader.azure_container}'...")
    all_sources = set()
    try:
        container_client = loader.blob_service_client.get_container_client(loader.azure_container)
        
        # Scan blobs to get a unique list of all source names
        for blob in container_client.list_blobs(name_starts_with="source_website="):
            if blob.name.endswith("/data.json"):
                try:
                    # Expected path: source_website={source_name}/scrape_date={date}/data.json
                    source_name = blob.name.split('/')[0].split('=')[1]
                    if source_name:
                        all_sources.add(source_name)
                except IndexError:
                    logger.debug(f"Skipping blob with unexpected path format: {blob.name}")
                    continue
        
        if not all_sources:
            logger.warning("No valid sources found in ADLS container. Nothing to load.")
            return

        # --- PREPARE SOURCES TO PROCESS FOR THE TARGET DATE ---
        sources_to_process = {source: target_date for source in all_sources}
        
        logger.info(f"Will attempt to load data for {len(sources_to_process)} sources for date {target_date}:")
        for source in sorted(sources_to_process.keys()):
              logger.info(f"   - {source}")

    except Exception as e:
        logger.error(f"❌ Failed to discover sources in ADLS: {e}")
        return
    
    # --- LOAD AND VALIDATE EACH SOURCE FOR THE CURRENT UTC DATE ---
    results = {}
    logger.info("\nStarting ADLS → BigQuery loading process...")
    for source, date_to_process in sorted(sources_to_process.items()):
        logger.info(f"--- Processing source: '{source}' for date: '{date_to_process}' ---")
        loaded_count = loader.load_from_adls_blob(source, date_to_process)
        results[source] = loaded_count

        if loaded_count > 0:
            logger.info(f"Validating load for '{source}' on '{date_to_process}'...")
            loader.validate_load(source, date_to_process)
            
            samples = loader.get_sample_products(source, date_to_process, limit=3)
            if samples:
                logger.info(f"Sample products:")
                for sample in samples:
                    logger.info(f"   • {sample.get('title')} ({sample.get('brand')})")
        else:
            logger.warning(f"No data was loaded for '{source}' on '{date_to_process}'.")
    
    logger.info("\nData loading completed!")

if __name__ == "__main__":
    main()

