import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _build_client(project_id: str, credentials_path: Path) -> bigquery.Client:
    """Create a BigQuery client using the provided service account key."""
    try:
        credentials = service_account.Credentials.from_service_account_file(str(credentials_path))
    except Exception as exc:
        raise RuntimeError(f"Failed to load service account credentials from {credentials_path}: {exc}") from exc

    return bigquery.Client(project=project_id, credentials=credentials)


def get_price_history(project_id: str, dataset_id: str, days: int = 90, credentials_path: Path | None = None) -> pd.DataFrame:
    """
    Extracts the price history for all variants from the BigQuery data warehouse
    for the specified number of past days.
    
    This function now includes a de-duplication step to handle cases where
    the source data may have multiple price entries for the same variant on the same day.
    """
    print(f"Extracting price history for the last {days} days...")
    
    # Resolve service account path and create the BigQuery client
    if credentials_path is None:
        return pd.DataFrame()

    resolved_path = Path(credentials_path).expanduser().resolve()
    if not resolved_path.exists():
        print(f"Service account file not found at {resolved_path}.")
        return pd.DataFrame()

    try:
        client = _build_client(project_id, resolved_path)
    except Exception as e:
        print(f"Error creating BigQuery client: {e}")
        return pd.DataFrame()

    # Define the time window for the query
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days)

    # Construct the SQL query to fetch data from the FactProductPrice table
    # This table is assumed to be the central source of daily price facts.
    query = f"""
        SELECT DISTINCT
            f.price_fact_id,
            f.variant_id,
            f.current_price,
            f.original_price,
            d.full_date
        FROM 
            `{project_id}.{dataset_id}.FactProductPrice` AS f
        JOIN
            `{project_id}.{dataset_id}.DimDate` AS d ON f.date_id = d.date_id
        WHERE 
            d.full_date BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    """

    try:
        # Execute the query and load the results into a pandas DataFrame
        df = client.query(query).to_dataframe()

        if df.empty:
            print("No price data found for the specified date range.")
            return pd.DataFrame()
            
        print(f"Successfully extracted {len(df)} raw price records from BigQuery.")

        # --- NEW DE-DUPLICATION LOGIC ---
        # Sort by price_fact_id descending. This assumes a higher ID is a later entry.
        df = df.sort_values('price_fact_id', ascending=False)
        
        # Keep only the FIRST record for each combination of variant_id and full_date.
        # Because we sorted, this will be the latest entry for that day.
        original_rows = len(df)
        df_cleaned = df.drop_duplicates(subset=['variant_id', 'full_date'], keep='first')
        cleaned_rows = len(df_cleaned)
        
        if original_rows > cleaned_rows:
            print(f"Data Cleaning: Removed {original_rows - cleaned_rows} duplicate price entries.")
            
        return df_cleaned

    except Exception as e:
        print(f"An error occurred while fetching data from BigQuery: {e}")
        return pd.DataFrame()

