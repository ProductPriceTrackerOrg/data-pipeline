#!/usr/bin/env python3
"""
Price Pulse Daily Data Pipeline DAG
Orchestrates the complete data flow from scraping to warehouse transformation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.bash import BashOperator
import os

# Default DAG arguments
default_args = {
    'owner': 'price-pulse-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 6),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

# Initialize DAG
dag = DAG(
    'price_pulse_daily_pipeline',
    default_args=default_args,
    description='Daily price tracking data pipeline',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['price-tracking', 'data-pipeline', 'bigquery']
)

# Project configuration
PROJECT_ID = 'price-pulse-470211'
BASE_PATH = '/opt/airflow/dags/transformations'

# Task 1: Update DimDate
update_dim_date = BigQueryInsertJobOperator(
    task_id='update_dim_date',
    configuration={
        "query": {
            "query": open(f"{BASE_PATH}/warehouse/sql/dim_date_update.sql").read(),
            "useLegacySql": False,
        }
    },
    project_id=PROJECT_ID,
    dag=dag
)

# Task 2: Run Data Loading (your existing loader.py)
def run_data_loader():
    """Execute the BigQuery loader for current day's data"""
    import subprocess
    import sys
    
    loader_path = f"{BASE_PATH}/loading/bigquery/loader.py"
    result = subprocess.run([sys.executable, loader_path], 
                          capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Loader failed: {result.stderr}")
    
    print(result.stdout)

load_staging_data = PythonOperator(
    task_id='load_staging_data',
    python_callable=run_data_loader,
    dag=dag
)

# Task 3: Transform DimShop
transform_dim_shop = BigQueryInsertJobOperator(
    task_id='transform_dim_shop',
    configuration={
        "query": {
            "query": open(f"{BASE_PATH}/warehouse/sql/dim_shop_upsert.sql").read(),
            "useLegacySql": False,
        }
    },
    project_id=PROJECT_ID,
    dag=dag
)

# Task 4: Data Quality Check
quality_check_staging = BigQueryCheckOperator(
    task_id='quality_check_staging',
    sql=f"""
    SELECT COUNT(*) 
    FROM `{PROJECT_ID}.staging.stg_raw_appleme`
    WHERE scrape_date = CURRENT_DATE()
    """,
    use_legacy_sql=False,
    dag=dag
)

# Task 5: Transform DimShopProduct (placeholder - we'll build this next)
transform_dim_shop_product = BigQueryInsertJobOperator(
    task_id='transform_dim_shop_product',
    configuration={
        "query": {
            "query": "SELECT 'DimShopProduct transformation - TO BE IMPLEMENTED' AS status",
            "useLegacySql": False,
        }
    },
    project_id=PROJECT_ID,
    dag=dag
)

# Task 6: Transform remaining dimensions and facts (placeholders)
transform_dim_variant = BigQueryInsertJobOperator(
    task_id='transform_dim_variant',
    configuration={
        "query": {
            "query": "SELECT 'DimVariant transformation - TO BE IMPLEMENTED' AS status",
            "useLegacySql": False,
        }
    },
    project_id=PROJECT_ID,
    dag=dag
)

transform_fact_product_price = BigQueryInsertJobOperator(
    task_id='transform_fact_product_price',
    configuration={
        "query": {
            "query": "SELECT 'FactProductPrice transformation - TO BE IMPLEMENTED' AS status",
            "useLegacySql": False,
        }
    },
    project_id=PROJECT_ID,
    dag=dag
)

# Define task dependencies
update_dim_date >> load_staging_data >> quality_check_staging >> transform_dim_shop >> transform_dim_shop_product >> transform_dim_variant >> transform_fact_product_price

if __name__ == "__main__":
    dag.cli()
