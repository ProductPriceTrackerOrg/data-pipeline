"""
DimDate Transformation Script
Generates and loads date dimension data into the warehouse.
"""

from datetime import datetime, timedelta
from typing import List, Dict
import logging
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DimDateTransformer:
    """
    Handles the transformation and loading of date dimension data.
    Generates a complete date dimension for a specified date range.
    """
    
    def __init__(self, project_id: str = "price-pulse-470211"):
        """
        Initialize the DimDate transformer.
        
        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = "warehouse"
        self.table_id = "DimDate"
        
    def generate_date_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """
        Generate date dimension data for the specified date range.
        
        Args:
            start_date: Start date for the dimension
            end_date: End date for the dimension
            
        Returns:
            List of dictionaries containing date dimension data
        """
        date_data = []
        current_date = start_date
        
        while current_date <= end_date:
            # Generate date_id as YYYYMMDD integer
            date_id = int(current_date.strftime("%Y%m%d"))
            
            # Get day of week name
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 
                        'Friday', 'Saturday', 'Sunday']
            day_of_week = day_names[current_date.weekday()]
            
            date_record = {
                'date_id': date_id,
                'full_date': current_date.strftime('%Y-%m-%d'),  # Convert to string for JSON
                'year': current_date.year,
                'month': current_date.month,
                'day': current_date.day,
                'day_of_week': day_of_week
            }
            
            date_data.append(date_record)
            current_date += timedelta(days=1)
            
        logger.info(f"Generated {len(date_data)} date records from {start_date.date()} to {end_date.date()}")
        return date_data
    
    def load_to_bigquery(self, date_data: List[Dict], truncate: bool = False) -> None:
        """
        Load date dimension data to BigQuery.
        
        Args:
            date_data: List of date dimension records
            truncate: If True, replace all existing data. If False, append only.
        """
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Configure job - truncate only if explicitly requested
        write_disposition = (bigquery.WriteDisposition.WRITE_TRUNCATE if truncate 
                            else bigquery.WriteDisposition.WRITE_APPEND)
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            schema=[
                bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("full_date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("day", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("day_of_week", "STRING", mode="REQUIRED"),
            ]
        )
        
        try:
            # Load data to BigQuery
            job = self.client.load_table_from_json(
                date_data, table_ref, job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            logger.info(f"Successfully loaded {len(date_data)} records to {table_ref}")
            
            # Verify the load
            table = self.client.get_table(table_ref)
            logger.info(f"Table {table_ref} now has {table.num_rows} rows")
            
        except Exception as e:
            logger.error(f"Failed to load data to {table_ref}: {e}")
            raise
    
    def transform_and_load(self, start_date: datetime = None, end_date: datetime = None, reset_table: bool = False) -> None:
        """
        Complete transformation and loading process for DimDate.
        
        Args:
            start_date: Start date for dimension (defaults to today)
            end_date: End date for dimension (defaults to today)
            reset_table: If True, replace all existing data. If False, add only if dates don't exist.
        """
        # Set default date range if not provided - only today's date
        if start_date is None:
            start_date = datetime.now()  # Start from today
        if end_date is None:
            end_date = datetime.now()    # End at today (only today's date)
            
        logger.info(f"Starting DimDate transformation for range: {start_date.date()} to {end_date.date()}")
        
        if reset_table:
            # Generate date data and replace all existing data
            date_data = self.generate_date_data(start_date, end_date)
            self.load_to_bigquery(date_data, truncate=True)
            logger.info("DimDate transformation completed successfully (table reset)")
        else:
            # Add dates only if they don't exist (safer approach)
            current_date = start_date
            while current_date <= end_date:
                self.add_date_if_not_exists(current_date)
                current_date += timedelta(days=1)
            logger.info("DimDate transformation completed successfully (incremental)")
    
    def add_date_if_not_exists(self, target_date: datetime) -> None:
        """
        Add a specific date to DimDate if it doesn't already exist.
        Useful for adding dates incrementally as we get new data.
        
        Args:
            target_date: Date to add to the dimension
        """
        date_id = int(target_date.strftime("%Y%m%d"))
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Check if date already exists
        check_query = f"""
        SELECT COUNT(*) as count
        FROM `{table_ref}`
        WHERE date_id = {date_id}
        """
        
        result = list(self.client.query(check_query).result())
        exists = result[0].count > 0
        
        if not exists:
            # Generate data for just this date
            date_data = self.generate_date_data(target_date, target_date)
            
            # Append to existing table
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("full_date", "DATE", mode="REQUIRED"),
                    bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("day", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("day_of_week", "STRING", mode="REQUIRED"),
                ]
            )
            
            job = self.client.load_table_from_json(
                date_data, table_ref, job_config=job_config
            )
            job.result()
            
            logger.info(f"Added new date {target_date.date()} (ID: {date_id}) to DimDate")
        else:
            logger.info(f"Date {target_date.date()} (ID: {date_id}) already exists in DimDate")

def main():
    """
    Main execution function for DimDate transformation.
    """
    try:
        transformer = DimDateTransformer()
        transformer.transform_and_load()
        
    except Exception as e:
        logger.error(f"DimDate transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()
