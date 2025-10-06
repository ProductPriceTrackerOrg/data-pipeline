import os
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas as pd
from google.oauth2 import service_account

# Load environment variables from the .env file
load_dotenv()

def get_unmatched_products() -> pd.DataFrame:
    """
    Loads products from DimShopProduct that do not have a corresponding
    entry in the FactProductMatch table.

    Returns:
        pd.DataFrame: A DataFrame with 'product_id' and 'product_title' columns.
    """
    project_id = os.getenv("PROJECT_ID")
    warehouse_id = os.getenv("WAREHOUSE")

    # Explicitly load credentials from your JSON file
    # Ensure 'gcp-credentials.json' is in your project's root directory
    credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gcp-credentials.json"))
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    
    if not project_id or not warehouse_id:
        raise ValueError("PROJECT_ID and WAREHOUSE must be set in your .env file")
    
    client = bigquery.Client(credentials=credentials, project=project_id)

    # Define the table names dynamically
    product_table = f"`{project_id}.{warehouse_id}.DimShopProduct`"
    match_table = f"`{project_id}.{warehouse_id}.FactProductMatch`"
    
    # --- CHANGE: New query to find products not in FactProductMatch ---
    query = f"""
        SELECT
            sp.shop_product_id AS product_id,
            sp.product_title_native AS product_title
        FROM
            {product_table} AS sp
        LEFT JOIN
            {match_table} AS fpm ON sp.shop_product_id = fpm.shop_product_id
        WHERE
            -- This condition is only true for products that don't have a match
            fpm.shop_product_id IS NULL
        ORDER BY
            sp.shop_product_id ASC
    """

    print(f"Executing query to find unmatched products...")
    
    query_job = client.query(query)
    dataframe = query_job.to_dataframe()

    print("Query complete. Data loaded into DataFrame.")
    
    return dataframe

if __name__ == "__main__":
    unmatched_products_df = get_unmatched_products()

    print("\n--- DataFrame Head ---")
    print(unmatched_products_df.head())

    print("\n--- DataFrame Info ---")
    unmatched_products_df.info()