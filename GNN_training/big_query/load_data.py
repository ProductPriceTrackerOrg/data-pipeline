import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account


# Load environment variables from .env file
load_dotenv()

def load_data_from_bigquery() -> pd.DataFrame:
    """
    Loads and joins product, category, and shop data from BigQuery
    in a single, efficient query.
    """
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
    
    # A single query to join all necessary tables is more efficient
    query = f"""
        SELECT
            sp.shop_product_id,
            sp.product_title_native,
            sp.brand_native,
            sp.predicted_master_category_id,
            sp.shop_id,
            c.category_name,
            s.shop_name
        FROM `{project_id}.{warehouse_id}.DimShopProduct` AS sp
        LEFT JOIN `{project_id}.{warehouse_id}.DimCategory` AS c
            ON sp.predicted_master_category_id = c.category_id
        LEFT JOIN `{project_id}.{warehouse_id}.DimShop` AS s
            ON sp.shop_id = s.shop_id
    """
    print("Executing query to fetch and join data from BigQuery...")
    dataframe = client.query(query).to_dataframe()
    print(f"Successfully loaded {len(dataframe)} rows.")
    return dataframe

def transform_data(df: pd.DataFrame) -> dict:
    """
    Performs all requested transformations on the raw DataFrame and
    returns a dictionary of the final, structured DataFrames.
    """
    print("Starting data transformations...")
    
    # --- Initial Data Cleaning and Preparation ---

    # 1. Impute missing brand names from the product title
    def impute_brand(row):
        if pd.isna(row['brand_native']):
            first_word = row['product_title_native'].split()[0]
            # Assign as brand only if the first word is not a number
            if not first_word.isdigit():
                return first_word
        return row['brand_native']
    
    df['brand_native'] = df.apply(impute_brand, axis=1)

    # 2. Remove duplicate products, keeping the first occurrence
    df.drop_duplicates(subset=['shop_product_id'], keep='first', inplace=True)
    
    # --- Create Final DataFrames ---
    
    # 3. product_df (product_id and product_title)
    product_df = df[['shop_product_id', 'product_title_native']].rename(columns={
        'shop_product_id': 'shop_product_id',
        'product_title_native': 'product_title_native'
    })
    
    # 4. brand_df (unique brands)
    unique_brands = df['brand_native'].dropna().unique()
    brand_df = pd.DataFrame(unique_brands, columns=["brand_native_name"])

    # 5. category_df (category_id | category_name)
    category_df = df[['predicted_master_category_id', 'category_name']].dropna().drop_duplicates().rename(columns={
        'predicted_master_category_id': 'category_id'  # Changed to category_id to be consistent
    })
    
    # 6. shop_df (shop_id | shop_name)
    shop_df = df[['shop_id', 'shop_name']].dropna().drop_duplicates()
    
    # 7. product_brand_edges_df (shop_product_id, brand_native_name)
    product_brand_edges_df = df[['shop_product_id', 'brand_native']].dropna().rename(columns={
        'brand_native': 'brand_native_name'
    })

    # 8. product_category_edges_df (shop_product_id, category_id)
    product_category_edges_df = df[['shop_product_id', 'predicted_master_category_id']].dropna().rename(columns={
        'predicted_master_category_id': 'category_id'  # Renamed to match category_df column
    })

    # 9. product_shop_edges_df (shop_product_id, shop_id)
    product_shop_edges_df = df[['shop_product_id', 'shop_id']].dropna()

    print("Transformations complete.")
    
    # Return all the created dataframes in a dictionary
    return {
        "products": product_df,
        "brands": brand_df,
        "categories": category_df,
        "shops": shop_df,
        "product_brand_edges": product_brand_edges_df,
        "product_category_edges": product_category_edges_df,
        "product_shop_edges": product_shop_edges_df
    }

if __name__ == "__main__":
    # Step 1: Load the raw data from BigQuery
    raw_df = load_data_from_bigquery()
    
    # Step 2: Perform all transformations
    final_dataframes = transform_data(raw_df)
    
    # Step 3: Display the head and info for each created DataFrame
    print("\n--- Generated DataFrames ---")
    for name, df in final_dataframes.items():
        print(f"\n--- DataFrame: {name} ---")
        print(df.head())
        print(df.info())

    # get df eperately
    product_df = final_dataframes['products']
    brand_df = final_dataframes['brands']
    category_df = final_dataframes['categories']
    shop_df = final_dataframes['shops']
    product_brand_edges_df = final_dataframes['product_brand_edges']
    product_category_edges_df = final_dataframes['product_category_edges']
    product_shop_edges_df = final_dataframes['product_shop_edges']

    print(product_df.columns)
    print(brand_df.columns)
    print(category_df.columns)
    print(shop_df.columns)
    print(product_brand_edges_df.columns)
    print(product_category_edges_df.columns)
    print(product_shop_edges_df.columns)
