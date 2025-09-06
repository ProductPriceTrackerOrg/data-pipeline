#!/usr/bin/env python3
from google.cloud import bigquery

client = bigquery.Client()
print("Available staging tables:")
dataset = client.dataset('staging')
tables = list(client.list_tables(dataset))
for table in tables:
    print(f"  {table.table_id}")

print("\nNow checking data for 2025-09-06:")
for table in tables:
    query = f"SELECT COUNT(*) as count FROM `price-pulse-470211.staging.{table.table_id}` WHERE scraped_date = '2025-09-06'"
    try:
        result = list(client.query(query).result())[0]
        print(f"  {table.table_id}: {result.count} rows")
    except Exception as e:
        print(f"  {table.table_id}: ERROR - {e}")
