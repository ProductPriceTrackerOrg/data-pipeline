import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account

# --- CHANGE 1: Load environment variables from the .env file ---
load_dotenv()

def get_uncategorized_products() -> pd.DataFrame:
    """
    Loads products with a NULL predicted_master_category_id from BigQuery
    into a pandas DataFrame, using credentials from a .env file.

    Returns:
        pd.DataFrame: A DataFrame with 'product_id' and 'product_title' columns.
    """
    # --- CHANGE 2: Get project and warehouse IDs from environment variables ---
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

    # --- CHANGE 4: Use an f-string to build the table name dynamically ---
    table_name = f"`{project_id}.{warehouse_id}.DimShopProduct`"
    
    query = f"""
        SELECT
            shop_product_id AS product_id,
            product_title_native AS product_title
        FROM
            {table_name}
        WHERE
            predicted_master_category_id IS NULL
    """

    print(f"Executing query on table: {table_name}...")
    
    query_job = client.query(query)
    dataframe = query_job.to_dataframe()

    print("Query complete. Data loaded into DataFrame.")
    
    return dataframe

if __name__ == "__main__":
    uncategorized_products_df = get_uncategorized_products()

    print("\n--- DataFrame Head ---")
    print(uncategorized_products_df.head())

    print("\n--- DataFrame Info ---")
    uncategorized_products_df.info()