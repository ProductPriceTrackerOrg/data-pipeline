"""
BigQuery module for querying price changes in the data warehouse.
This module is responsible for finding products with price changes.
"""

import os
from google.cloud import bigquery
from typing import List, Dict

def get_price_changes() -> List[Dict]:
    """
    Queries a BigQuery data warehouse to find products with price changes.
    
    This function:
    1. Initializes the BigQuery client.
    2. Loads GCP_PROJECT_ID and BIGQUERY_DATASET_ID from environment variables.
    3. Constructs a SQL query that compares the latest price with the immediately preceding price for each variant.
    4. The SQL query uses the LAG() window function over a partition of variant_id, ordered by date_id.
    5. The query joins FactProductPrice, DimVariant, and DimShopProduct tables.
    6. Selects variant_id, product_title_native, product_url, current_price (new_price), and previous_price (old_price).
    7. Filters results for the most recent date_id only and where the price has changed.
    8. Executes the query and returns the results as a list of dictionaries.
    
    Returns:
        List[Dict]: A list of dictionaries containing product information and price changes
    """
    print("Initializing BigQuery client for price change detection...")
    
    # Get project and dataset IDs from environment variables
    gcp_project_id = os.getenv("PROJECT_ID")
    bigquery_dataset_id = os.getenv("WAREHOUSE")
    
    if not gcp_project_id or not bigquery_dataset_id:
        print("Error: Missing required environment variables (PROJECT_ID, WAREHOUSE)")
        return []
    
    # Initialize the BigQuery client
    client = bigquery.Client()
    
    # Construct the SQL query using window functions to compare current and previous prices
    sql_query = f"""
    WITH RankedPrices AS (
        SELECT
            v.variant_id,
            sp.shop_product_id,
            sp.product_title_native,
            sp.product_url,
            fpp.date_id,
            fpp.current_price,
            LAG(fpp.current_price, 1) OVER (PARTITION BY v.variant_id ORDER BY fpp.date_id) AS previous_price
        FROM
            `{gcp_project_id}.{bigquery_dataset_id}.FactProductPrice` AS fpp
        JOIN
            `{gcp_project_id}.{bigquery_dataset_id}.DimVariant` AS v ON fpp.variant_id = v.variant_id
        JOIN
            `{gcp_project_id}.{bigquery_dataset_id}.DimShopProduct` AS sp ON v.shop_product_id = sp.shop_product_id
    )
    SELECT
        variant_id,
        shop_product_id,
        product_title_native,
        product_url,
        current_price AS new_price,
        previous_price AS old_price
    FROM
        RankedPrices
    WHERE
        date_id = (SELECT MAX(date_id) FROM `{gcp_project_id}.{bigquery_dataset_id}.FactProductPrice`)
        AND current_price IS NOT NULL
        AND previous_price IS NOT NULL
        AND ABS(current_price - previous_price) > 0.01  -- Ensuring we catch actual changes (avoid float precision issues)
    """
    
    print("Querying BigQuery data warehouse for price changes...")
    try:
        query_job = client.query(sql_query)
        results = [dict(row) for row in query_job.result()]
        print(f"Found {len(results)} products with price changes.")
        
        # Log a preview of the results for debugging
        if results:
            print(f"Sample price change - Product: {results[0]['product_title_native']}, " 
                  f"Old Price: {results[0]['old_price']}, New Price: {results[0]['new_price']}")
        
        return results
    except Exception as e:
        print(f"Error querying BigQuery: {str(e)}")
        return []