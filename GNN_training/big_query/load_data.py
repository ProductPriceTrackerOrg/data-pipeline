import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from supabase import create_client, Client # <-- NEW: Import Supabase

# Load environment variables from .env file
load_dotenv()

def load_data_from_bigquery() -> pd.DataFrame:
    """
    Loads and joins product, category, and shop data from BigQuery.
    """
    project_id = os.getenv("PROJECT_ID")
    warehouse_id = os.getenv("WAREHOUSE")
    credentials_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gcp-credentials.json"))
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    
    if not project_id or not warehouse_id:
        raise ValueError("PROJECT_ID and WAREHOUSE must be set in your .env file")
    
    client = bigquery.Client(credentials=credentials, project=project_id)
    
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
    print(f"Successfully loaded {len(dataframe)} rows from BigQuery.")
    return dataframe

# --- NEW FUNCTION ---
def load_data_from_supabase() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads the profiles and user activity logs from the Supabase database.
    """
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in your .env file")

    print("Connecting to Supabase...")
    supabase: Client = create_client(url, key)
    
    # Fetch profiles data
    print("Fetching 'profiles' table...")
    profiles_response = supabase.table('profiles').select("user_id, full_name, email").execute()
    profiles_df = pd.DataFrame(profiles_response.data)
    print(f"Successfully loaded {len(profiles_df)} rows from 'profiles'.")
    
    # Fetch user activity data
    print("Fetching 'useractivitylog' table...")
    activity_response = supabase.table('useractivitylog').select("user_id, activity_type, variant_id, activity_timestamp").execute()
    activity_df = pd.DataFrame(activity_response.data)
    print(f"Successfully loaded {len(activity_df)} rows from 'useractivitylog'.")
    
    return profiles_df, activity_df

# --- UPDATED FUNCTION ---
def transform_data(df: pd.DataFrame, profiles_df: pd.DataFrame, activity_df: pd.DataFrame) -> dict:
    """
    Performs all transformations on the raw DataFrames and
    returns a dictionary of the final, structured DataFrames.
    """
    print("Starting data transformations...")
    
    # --- BigQuery Data Cleaning ---
    def impute_brand(row):
        if pd.isna(row['brand_native']):
            first_word = row['product_title_native'].split()[0]
            if not first_word.isdigit():
                return first_word
        return row['brand_native']
    df['brand_native'] = df.apply(impute_brand, axis=1)
    df.drop_duplicates(subset=['shop_product_id'], keep='first', inplace=True)
    
    # --- Create Final DataFrames ---
    
    # DataFrames from BigQuery
    product_df = df[['shop_product_id', 'product_title_native']]
    unique_brands = df['brand_native'].dropna().unique()
    brand_df = pd.DataFrame(unique_brands, columns=["brand_native_name"])
    category_df = df[['predicted_master_category_id', 'category_name']].dropna().drop_duplicates().rename(columns={
        'predicted_master_category_id': 'category_id'
    })
    shop_df = df[['shop_id', 'shop_name']].dropna().drop_duplicates()
    product_brand_edges_df = df[['shop_product_id', 'brand_native']].dropna().rename(columns={
        'brand_native': 'brand_native_name'
    })
    product_category_edges_df = df[['shop_product_id', 'predicted_master_category_id']].dropna().rename(columns={
        'predicted_master_category_id': 'category_id'
    })
    product_shop_edges_df = df[['shop_product_id', 'shop_id']].dropna()
    
    # --- NEW: DataFrames from Supabase ---
    
    # 10. users_df (user_id, full_name, email)
    # This is our user node data.
    users_df = profiles_df[['user_id', 'full_name', 'email']].dropna(subset=['user_id']).drop_duplicates(subset=['user_id'])
    
    # 11. user_product_edges_df (user_id, shop_product_id)
    # This represents the relationship between users and products.
    # We'll filter for a meaningful interaction like 'add_favorite'.
    user_product_edges_df = activity_df[activity_df['activity_type'] == 'add_favorite'].copy()
    user_product_edges_df = user_product_edges_df[['user_id', 'variant_id', 'activity_timestamp']].dropna()
    # As requested, rename 'variant_id' to 'shop_product_id' for consistency.
    user_product_edges_df = user_product_edges_df.rename(columns={'variant_id': 'shop_product_id'})

    print("Transformations complete.")
    
    return {
        "products": product_df,
        "brands": brand_df,
        "categories": category_df,
        "shops": shop_df,
        "users": users_df, # <-- NEW
        "product_brand_edges": product_brand_edges_df,
        "product_category_edges": product_category_edges_df,
        "product_shop_edges": product_shop_edges_df,
        "user_product_edges": user_product_edges_df # <-- NEW
    }

if __name__ == "__main__":
    # --- UPDATED MAIN BLOCK ---
    
    # Step 1: Load data from both sources
    raw_bq_df = load_data_from_bigquery()
    raw_profiles_df, raw_activity_df = load_data_from_supabase()
    
    # Step 2: Perform all transformations
    final_dataframes = transform_data(raw_bq_df, raw_profiles_df, raw_activity_df)
    
    # Step 3: Display the head and info for each created DataFrame
    print("\n--- Generated DataFrames ---")
    for name, df in final_dataframes.items():
        print(f"\n--- DataFrame: {name} ---")
        print(df.head())
        print(f"Info for {name}:")
        df.info()

    # Get df separately
    product_df = final_dataframes['products']
    brand_df = final_dataframes['brands']
    category_df = final_dataframes['categories']
    shop_df = final_dataframes['shops']
    users_df = final_dataframes['users'] # <-- NEW
    product_brand_edges_df = final_dataframes['product_brand_edges']
    product_category_edges_df = final_dataframes['product_category_edges']
    product_shop_edges_df = final_dataframes['product_shop_edges']
    user_product_edges_df = final_dataframes['user_product_edges'] # <-- NEW


    # # save data frames
    # product_df.to_csv("products.csv", index=False)
    # brand_df.to_csv("brands.csv", index=False)
    # category_df.to_csv("categories.csv", index=False)
    # shop_df.to_csv("shops.csv", index=False)
    # users_df.to_csv("users.csv", index=False) # <-- NEW
    # product_brand_edges_df.to_csv("product_brand_edges.csv", index=False)
    # product_category_edges_df.to_csv("product_category_edges.csv", index=False)
    # product_shop_edges_df.to_csv("product_shop_edges.csv", index=False)
    # user_product_edges_df.to_csv("user_product_edges.csv", index=False) # <-- NEW

    print("\n--- DataFrame Columns ---")
    print("products:", product_df.columns)
    print("brands:", brand_df.columns)
    print("categories:", category_df.columns)
    print("shops:", shop_df.columns)
    print("users:", users_df.columns) # <-- NEW
    print("product_brand_edges:", product_brand_edges_df.columns)
    print("product_category_edges:", product_category_edges_df.columns)
    print("product_shop_edges:", product_shop_edges_df.columns)
    print("user_product_edges:", user_product_edges_df.columns) # <-- NEW