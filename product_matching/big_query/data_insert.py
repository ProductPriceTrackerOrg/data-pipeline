import os
import pandas as pd
import io
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Load environment variables from .env file
load_dotenv()

def upload_matches_to_bigquery(matches_df: pd.DataFrame):
    """
    Prepares and uploads the product match results to a BigQuery table.

    Args:
        matches_df: DataFrame with match_group_id, shop_product_id, confidence_score.
    """
    if matches_df.empty:
        print("Input DataFrame is empty. Nothing to upload.")
        return

    # --- Step 1: Prepare the DataFrame to match the BigQuery Schema ---
    print("Preparing DataFrame for upload...")

    # Add the static model_id
    matches_df['model_id'] = 1

    # Generate a unique match_id for each row.
    # A simple way for a single batch is to use the index.
    # For multiple runs, a more robust method like UUIDs or a timestamp might be needed.
    matches_df.reset_index(inplace=True)
    matches_df = matches_df.rename(columns={'index': 'match_id'})

    # Ensure the column order matches the target table schema
    final_df = matches_df[[
        'match_id',
        'match_group_id',
        'shop_product_id',
        'model_id',
        'confidence_score'
    ]]
    
    print("DataFrame prepared with the following columns:")
    print(final_df.info())

    # --- Step 2: Configure and Run the BigQuery Load Job ---
    project_id = os.getenv("PROJECT_ID")
    warehouse_id = os.getenv("WAREHOUSE")

    # Explicitly load credentials from your JSON file
    # Ensure 'gcp-credentials.json' is in your project's root directory
    credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gcp-credentials.json"))
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    
    if not project_id or not warehouse_id:
        raise ValueError("PROJECT_ID and WAREHOUSE must be set in your .env file")
    
    client = bigquery.Client(credentials=credentials, project=project_id)
    table_id = f"{project_id}.{warehouse_id}.FactProductMatch"
    
    # Configure the job to APPEND data to the existing table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    print(f"\nUploading {len(final_df)} rows to {table_id}...")
    
    # Start the load job
    load_job = client.load_table_from_dataframe(
        final_df, table_id, job_config=job_config
    )
    
    load_job.result()  # Wait for the job to complete

    print(f"Success! {load_job.output_rows} rows have been added to the table.")


