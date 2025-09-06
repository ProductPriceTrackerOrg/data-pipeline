#!/usr/bin/env python3
"""
BigQuery staging table schemas and utilities
"""
from google.cloud import bigquery
from typing import List

# Standard schema for all staging tables
STAGING_SCHEMA = [
    bigquery.SchemaField("raw_json_data", "JSON", mode="REQUIRED", description="Complete product array JSON data from ADLS"),
    bigquery.SchemaField("scrape_date", "DATE", mode="REQUIRED", description="Date when data was scraped"),
    bigquery.SchemaField("source_website", "STRING", mode="REQUIRED", description="Source website identifier"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED", description="Timestamp when data was loaded to BigQuery"),
    bigquery.SchemaField("file_path", "STRING", mode="NULLABLE", description="Original file path in ADLS"),
    bigquery.SchemaField("product_count", "INTEGER", mode="REQUIRED", description="Number of products in the JSON array"),
    bigquery.SchemaField("overwrite_timestamp", "FLOAT", mode="NULLABLE", description="Timestamp for handling streaming buffer overwrites"),
]

def get_staging_table_name(source: str) -> str:
    """Generate standardized staging table name for a source"""
    # Clean source name: remove dots, hyphens, convert to lowercase
    clean_source = source.replace('.', '_').replace('-', '_').lower()
    return f"stg_raw_{clean_source}"

def get_staging_table_id(project_id: str, dataset: str, source: str) -> str:
    """Generate full BigQuery table ID"""
    table_name = get_staging_table_name(source)
    return f"{project_id}.{dataset}.{table_name}"

def create_staging_table_ddl(project_id: str, dataset: str, source: str) -> str:
    """Generate DDL to create staging table for a source"""
    table_name = get_staging_table_name(source)
    table_id = get_staging_table_id(project_id, dataset, source)
    
    return f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        raw_json_data JSON NOT NULL OPTIONS(description="Complete product array JSON data from ADLS"),
        scrape_date DATE NOT NULL OPTIONS(description="Date when data was scraped"),
        source_website STRING NOT NULL OPTIONS(description="Source website identifier"),
        loaded_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when data was loaded to BigQuery"),
        file_path STRING OPTIONS(description="Original file path in ADLS"),
        product_count INTEGER NOT NULL OPTIONS(description="Number of products in the JSON array")
    )
    PARTITION BY scrape_date
    CLUSTER BY source_website
    OPTIONS (
        description = "Raw scraped data staging table for {source} - one row per website per date",
        partition_expiration_days = 365,
        labels = [("source", "{source.lower()}"), ("layer", "staging"), ("data_type", "raw")]
    )
    """

def get_all_expected_staging_tables(project_id: str, dataset: str, sources: List[str]) -> List[str]:
    """Get list of all expected staging table IDs for given sources"""
    return [get_staging_table_id(project_id, dataset, source) for source in sources]

def validate_staging_table_schema(table: bigquery.Table) -> List[str]:
    """Validate that a staging table has the correct schema"""
    issues = []
    
    # Check if all required fields exist
    table_fields = {field.name: field for field in table.schema}
    
    for expected_field in STAGING_SCHEMA:
        if expected_field.name not in table_fields:
            issues.append(f"Missing field: {expected_field.name}")
        else:
            actual_field = table_fields[expected_field.name]
            if actual_field.field_type != expected_field.field_type:
                issues.append(f"Field {expected_field.name}: expected {expected_field.field_type}, got {actual_field.field_type}")
            if actual_field.mode != expected_field.mode:
                issues.append(f"Field {expected_field.name}: expected mode {expected_field.mode}, got {actual_field.mode}")
    
    return issues

def get_data_validation_query(table_id: str, date: str) -> str:
    """Generate query to validate data in staging table - works with JSON arrays"""
    return f"""
    SELECT 
        source_website,
        scrape_date,
        COUNT(*) as row_count,
        SUM(product_count) as total_products,
        AVG(product_count) as avg_products_per_source,
        MIN(loaded_at) as first_loaded,
        MAX(loaded_at) as last_loaded,
        STRING_AGG(DISTINCT file_path) as file_paths
    FROM `{table_id}`
    WHERE scrape_date = '{date}'
    GROUP BY source_website, scrape_date
    ORDER BY source_website
    """

def get_sample_products_query(table_id: str, date: str, limit: int = 5) -> str:
    """Generate query to get sample products for inspection - unnests JSON arrays"""
    return f"""
    SELECT 
        JSON_EXTRACT_SCALAR(product, '$.product_id_native') as product_id,
        JSON_EXTRACT_SCALAR(product, '$.product_title') as title,
        JSON_EXTRACT_SCALAR(product, '$.brand') as brand,
        source_website,
        ARRAY_LENGTH(JSON_EXTRACT_ARRAY(product, '$.variants')) as variant_count,
        loaded_at
    FROM `{table_id}`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
    WHERE scrape_date = '{date}'
    LIMIT {limit}
    """

def get_staging_table_info():
    """Get information about staging table design"""
    return {
        'purpose': 'Store raw JSON data from ADLS with minimal transformation',
        'partitioning': 'BY scrape_date for efficient date-based queries',
        'clustering': 'BY source_website for efficient source-based filtering',
        'retention': '365 days partition expiration',
        'schema': [
            {
                'field': field.name,
                'type': field.field_type,
                'mode': field.mode,
                'description': field.description
            }
            for field in STAGING_SCHEMA
        ]
    }

if __name__ == "__main__":
    # Demo the schema utilities
    print("🏗️ BigQuery Staging Schema Utilities")
    print("=" * 50)
    
    # Test table name generation
    test_sources = ["appleme", "cyberdeals.lk", "simplytek-store"]
    print("Table name generation:")
    for source in test_sources:
        print(f"  {source} -> {get_staging_table_name(source)}")
    
    print(f"\nStaging schema has {len(STAGING_SCHEMA)} fields:")
    for field in STAGING_SCHEMA:
        print(f"  {field.name} ({field.field_type}, {field.mode})")
    
    # Sample DDL
    print(f"\nSample DDL for 'appleme':")
    print(create_staging_table_ddl("project-id", "staging", "appleme"))