"""
Transformation Utilities
Common functions and utilities used across warehouse transformations.
"""

import hashlib
from typing import Any, Dict, List
from datetime import datetime
import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)

def generate_md5_id(*args) -> str:
    """
    Generate MD5 hash ID from concatenated arguments.
    This replicates BigQuery's TO_HEX(MD5(CONCAT(...))) pattern.
    
    Args:
        *args: Values to concatenate and hash
        
    Returns:
        32-character hexadecimal MD5 hash
    """
    # Convert all arguments to strings and concatenate
    concat_string = ''.join(str(arg) for arg in args if arg is not None)
    
    # Generate MD5 hash
    md5_hash = hashlib.md5(concat_string.encode('utf-8')).hexdigest()
    
    return md5_hash.upper()  # Return uppercase to match BigQuery TO_HEX

def get_current_date_id() -> int:
    """
    Get current date as integer ID in YYYYMMDD format.
    
    Returns:
        Current date as integer (e.g., 20250906)
    """
    return int(datetime.now().strftime("%Y%m%d"))

def create_bigquery_client(project_id: str = "price-pulse-470211") -> bigquery.Client:
    """
    Create and return BigQuery client.
    
    Args:
        project_id: Google Cloud project ID
        
    Returns:
        Configured BigQuery client
    """
    return bigquery.Client(project=project_id)

def execute_query(client: bigquery.Client, query: str) -> List[Dict]:
    """
    Execute BigQuery query and return results as list of dictionaries.
    
    Args:
        client: BigQuery client
        query: SQL query to execute
        
    Returns:
        List of dictionaries containing query results
    """
    try:
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert to list of dictionaries
        return [dict(row) for row in results]
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        logger.error(f"Query: {query}")
        raise

def load_data_to_table(
    client: bigquery.Client,
    data: List[Dict],
    table_ref: str,
    schema: List[bigquery.SchemaField],
    write_disposition: str = "WRITE_TRUNCATE"
) -> None:
    """
    Load data to BigQuery table.
    
    Args:
        client: BigQuery client
        data: List of dictionaries to load
        table_ref: Full table reference (project.dataset.table)
        schema: Table schema definition
        write_disposition: How to handle existing data (WRITE_TRUNCATE, WRITE_APPEND)
    """
    job_config = bigquery.LoadJobConfig(
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
        schema=schema
    )
    
    try:
        job = client.load_table_from_json(data, table_ref, job_config=job_config)
        job.result()  # Wait for completion
        
        # Verify load
        table = client.get_table(table_ref)
        logger.info(f"Successfully loaded {len(data)} records to {table_ref}")
        logger.info(f"Table now has {table.num_rows} total rows")
        
    except Exception as e:
        logger.error(f"Failed to load data to {table_ref}: {e}")
        raise

def get_staging_data(client: bigquery.Client, table_name: str, date_filter: str = None) -> List[Dict]:
    """
    Fetch data from staging tables.
    
    Args:
        client: BigQuery client
        table_name: Name of the staging table
        date_filter: Optional date filter (e.g., "scraped_date = CURRENT_DATE()")
        
    Returns:
        List of dictionaries containing staging data
    """
    query = f"""
    SELECT *
    FROM `price-pulse-470211.staging.{table_name}`
    """
    
    if date_filter:
        query += f" WHERE {date_filter}"
        
    return execute_query(client, query)

class TransformationBase:
    """
    Base class for all transformation classes.
    Provides common functionality and patterns.
    """
    
    def __init__(self, project_id: str = "price-pulse-470211"):
        """
        Initialize base transformer.
        
        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        self.client = create_bigquery_client(project_id)
        self.staging_dataset = "staging"
        self.warehouse_dataset = "warehouse"
        
    def get_table_ref(self, table_name: str, dataset: str = None) -> str:
        """
        Get full table reference.
        
        Args:
            table_name: Name of the table
            dataset: Dataset name (defaults to warehouse)
            
        Returns:
            Full table reference string
        """
        if dataset is None:
            dataset = self.warehouse_dataset
            
        return f"{self.project_id}.{dataset}.{table_name}"
    
    def log_transformation_start(self, table_name: str) -> None:
        """Log transformation start."""
        logger.info(f"Starting {table_name} transformation")
        
    def log_transformation_complete(self, table_name: str, record_count: int) -> None:
        """Log transformation completion."""
        logger.info(f"{table_name} transformation completed successfully - {record_count} records processed")
