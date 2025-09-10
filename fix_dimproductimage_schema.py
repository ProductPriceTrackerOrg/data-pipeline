"""
Update DimProductImage table schema to fix shop_product_id data type
"""
from google.cloud import bigquery

def update_table_schema():
    client = bigquery.Client(project="price-pulse-470211")
    
    print("🔧 Updating DimProductImage table schema...")
    
    # Drop and recreate the table with correct schema
    table_id = "price-pulse-470211.warehouse.DimProductImage"
    
    try:
        # Drop table if exists
        client.delete_table(table_id, not_found_ok=True)
        print("   Dropped existing table")
        
        # Create table with correct schema
        schema = [
            bigquery.SchemaField("image_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("shop_product_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("image_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("sort_order", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("scraped_date", "DATE", mode="NULLABLE"),
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        table.description = "Dimension table for product images from shop listings."
        
        table = client.create_table(table)
        print(f"✅ Created table {table.table_id} with correct schema")
        
        # Show schema
        print("\n📋 New table schema:")
        for field in table.schema:
            print(f"   {field.name}: {field.field_type} ({field.mode})")
            
    except Exception as e:
        print(f"❌ Error updating table schema: {e}")

if __name__ == "__main__":
    update_table_schema()
