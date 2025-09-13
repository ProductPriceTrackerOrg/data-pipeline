import os
import uuid
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Load environment variables from .env file
load_dotenv()

def update_categories_in_bigquery(predictions_df: pd.DataFrame) -> dict:
    """
    Updates the DimShopProduct table in BigQuery with predicted categories
    from a pandas DataFrame using an efficient MERGE statement.

    Args:
        predictions_df: DataFrame with 'product_id' and 'predicted_category_id'.

    Returns:
        A dictionary with the counts of updated and not-found rows.
    """
    if predictions_df.empty:
        print("Input DataFrame is empty. No updates to perform.")
        return {"updated_count": 0, "not_found_count": 0}

    # --- Step 1: Setup BigQuery Client and Config ---
    project_id = os.getenv("PROJECT_ID")
    warehouse_id = os.getenv("WAREHOUSE")
    # --- CHANGE: Explicitly load credentials from your JSON file ---
    credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gcp-credentials.json"))
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    

    # --- CHANGE 3: Add validation to ensure the .env file was loaded ---
    if not project_id or not warehouse_id:
        raise ValueError("PROJECT_ID and WAREHOUSE must be set in your .env file")
    
    # Instantiate the BigQuery client
    client = bigquery.Client(credentials=credentials, project=project_id)
    target_table_id = f"{project_id}.{warehouse_id}.DimShopProduct"
    
    # Create a unique name for the temporary source table
    temp_table_name = f"temp_predictions_{str(uuid.uuid4()).replace('-', '')}"
    temp_table_id = f"{project_id}.{warehouse_id}.{temp_table_name}"

    try:
        # --- Step 2: Upload DataFrame to a Temporary Table in BigQuery ---
        print(f"Uploading {len(predictions_df)} predictions to temporary table: {temp_table_id}...")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # Overwrite the temp table if it exists
        )
        client.load_table_from_dataframe(
            predictions_df, temp_table_id, job_config=job_config
        ).result()
        print("Upload complete.")

        # --- Step 3: Construct and Run the MERGE Query ---
        # This single query updates all matching rows in one operation.
        merge_query = f"""
            MERGE `{target_table_id}` AS T
            USING `{temp_table_id}` AS S
            ON T.shop_product_id = S.product_id
            WHEN MATCHED THEN
              UPDATE SET T.predicted_master_category_id = S.predicted_category_id
        """

        print("Executing MERGE statement to update categories...")
        merge_job = client.query(merge_query)
        merge_job.result()  # Wait for the job to complete
        
        # --- Step 4: Get Final Metrics ---
        updated_count = merge_job.num_dml_affected_rows
        not_found_count = len(predictions_df) - updated_count
        
        print("MERGE operation complete.")
        
        return {
            "updated_count": updated_count,
            "not_found_count": not_found_count
        }

    finally:
        # --- Step 5: Clean Up - Delete the Temporary Table ---
        print(f"Deleting temporary table: {temp_table_id}...")
        client.delete_table(temp_table_id, not_found_ok=True)
        print("Cleanup complete.")


if __name__ == "__main__":
    # This is your sample DataFrame with the prediction results.
    data = {
        'product_id': [1, 2, 3, 4, 5, 6], # Added a non-existent ID for testing
        'predicted_category_id': [13, 10, 22, 15, 4, 9]
    }
    final_predictions_df = pd.DataFrame(data)

    # Run the update process
    metrics = update_categories_in_bigquery(final_predictions_df)
    
    print("\n--- Final Report ---")
    print(f"Successfully Updated: {metrics['updated_count']} products")
    print(f"Product IDs Not Found: {metrics['not_found_count']} products")