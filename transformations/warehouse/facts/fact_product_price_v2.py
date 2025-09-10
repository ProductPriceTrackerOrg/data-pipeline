"""
FactProductPrice Loader Script
Extracts daily price and availability for each product variant from BigQuery staging tables and loads to warehouse.
"""
import json
import hashlib
from datetime import datetime
from google.cloud import bigquery

# Config
PROJECT_ID = "price-pulse-470211"
STAGING_DATASET = "staging"
WAREHOUSE_DATASET = "warehouse"
TABLE = "FactProductPrice"

# Staging tables to process
STAGING_TABLES = [
    "stg_raw_appleme",
    "stg_raw_simplytek", 
    "stg_raw_onei_lk"
]

# Helper functions
def get_variant_id(source_website, variant_id_native):
    """Generate variant_id using MD5 hash"""
    key = f"{source_website}|{variant_id_native}"
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16) % (10**12)

def get_date_id(scrape_timestamp):
    """Convert timestamp to YYYYMMDD format"""
    dt = datetime.fromisoformat(scrape_timestamp[:10])
    return int(dt.strftime("%Y%m%d"))

def is_available(text):
    """Check if product is available based on text"""
    if not text:
        return False
    text = text.lower()
    return ("in stock" in text or "✓" in text) and "sold out" not in text

def parse_price(price_str):
    """Parse price string, handling commas and other formatting issues"""
    if not price_str or price_str == '':
        return None
    
    # Convert to string and clean it
    price_str = str(price_str).strip()
    
    # Remove commas (thousands separators)
    price_str = price_str.replace(',', '')
    
    # Remove any currency symbols
    price_str = price_str.replace('LKR', '').replace('Rs', '').replace('$', '').strip()
    
    # Try to convert to float
    try:
        return float(price_str)
    except (ValueError, TypeError):
        return None

def extract_from_staging_tables(client):
    """Extract product data from BigQuery staging tables"""
    all_products = []
    
    for table_name in STAGING_TABLES:
        print(f"📊 Extracting data from {table_name}...")
        
        # Query to extract product data from staging table
        query = f"""
        SELECT 
            raw_json_data,
            inserted_at
        FROM `{PROJECT_ID}.{STAGING_DATASET}.{table_name}`
        WHERE raw_json_data IS NOT NULL
        ORDER BY inserted_at DESC
        """
        
        try:
            results = client.query(query).result()
            count = 0
            
            for row in results:
                try:
                    # Parse the JSON data
                    product_data = json.loads(row.raw_json_data)
                    
                    # Add metadata if not present
                    if 'metadata' not in product_data:
                        product_data['metadata'] = {}
                    
                    # Ensure we have source_website and scrape_timestamp
                    if 'source_website' not in product_data['metadata']:
                        # Extract from table name (e.g., stg_raw_appleme -> appleme.lk)
                        if 'appleme' in table_name:
                            website = 'appleme.lk'
                        elif 'simplytek' in table_name:
                            website = 'simplytek.lk'
                        elif 'onei' in table_name:
                            website = 'onei.lk'
                        else:
                            website = table_name.replace('stg_raw_', '') + '.lk'
                        product_data['metadata']['source_website'] = website
                    
                    if 'scrape_timestamp' not in product_data['metadata']:
                        # Use inserted_at as fallback
                        product_data['metadata']['scrape_timestamp'] = row.inserted_at.isoformat()
                    
                    all_products.append(product_data)
                    count += 1
                    
                except json.JSONDecodeError as e:
                    print(f"  ⚠ Failed to parse JSON from {table_name}: {e}")
                    continue
                    
            print(f"  ✅ Extracted {count} products from {table_name}")
            
        except Exception as e:
            print(f"  ❌ Error querying {table_name}: {e}")
            continue
    
    print(f"🎯 Total products extracted from staging: {len(all_products)}")
    return all_products

def main():
    print("🚀 Starting FactProductPrice extraction from staging tables...\n")
    
    # Initialize BigQuery client
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print("✅ BigQuery client initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize BigQuery client: {e}")
        return

    # Extract products from staging tables
    products = extract_from_staging_tables(client)
    
    if not products:
        print("❌ No products found in staging tables")
        return

    # Log sample product structure
    if products:
        print("\n📋 Sample product structure:")
        sample = products[0]
        print(f"  Keys: {list(sample.keys())}")
        if 'metadata' in sample:
            metadata = sample['metadata']
            print(f"  Source: {metadata.get('source_website', 'N/A')}")
            print(f"  Timestamp: {metadata.get('scrape_timestamp', 'N/A')}")
        if 'variants' in sample and sample['variants']:
            variant = sample['variants'][0]
            print(f"  Sample variant: ID={variant.get('variant_id_native')}, Price={variant.get('price_current')}")

    # Get current max price_fact_id
    print(f"\n🔢 Getting next price_fact_id...")
    query = f"""
    SELECT COALESCE(MAX(price_fact_id), -1) + 1 AS next_id
    FROM `{PROJECT_ID}.{WAREHOUSE_DATASET}.{TABLE}`
    """
    try:
        query_result = list(client.query(query).result())
        next_id = query_result[0].next_id if query_result else 0
        print(f"  ✅ Starting from price_fact_id: {next_id}")
    except Exception as e:
        print(f"  ⚠ Warning: Could not query existing table, starting from 0: {e}")
        next_id = 0

    # Process products
    print(f"\n⚙️ Processing {len(products)} products...")
    rows = []
    skipped_products = 0
    skipped_variants = 0
    null_price_variants = 0
    
    for i, product in enumerate(products):
        if i % 500 == 0 and i > 0:
            print(f"  📊 Processed {i}/{len(products)} products...")
            
        if not isinstance(product, dict):
            skipped_products += 1
            continue
            
        metadata = product.get("metadata", {})
        variants = product.get("variants", [])
        source_website = metadata.get("source_website")
        scrape_timestamp = metadata.get("scrape_timestamp")

        if not source_website or not scrape_timestamp:
            skipped_products += 1
            continue

        try:
            date_id = get_date_id(scrape_timestamp)
        except Exception as e:
            skipped_products += 1
            continue

        for j, variant in enumerate(variants):
            if not isinstance(variant, dict):
                skipped_variants += 1
                continue
                
            variant_id_native = variant.get("variant_id_native")
            price_current = variant.get("price_current")
            price_original = variant.get("price_original")
            availability_text = variant.get("availability_text", "")

            if not variant_id_native:
                skipped_variants += 1
                continue

            try:
                variant_id = get_variant_id(source_website, variant_id_native)
                current_price = parse_price(price_current)
                original_price = parse_price(price_original) if price_original else None
                
                # Handle NULL prices - set to 0.0 to avoid BigQuery schema issues
                if current_price is None:
                    null_price_variants += 1
                    current_price = 0.0  # Use 0.0 instead of NULL
                
                available = is_available(availability_text)

                row = {
                    "price_fact_id": next_id,
                    "variant_id": variant_id,
                    "date_id": date_id,
                    "current_price": current_price,  # Always a float now
                    "original_price": original_price,
                    "is_available": available
                }
                rows.append(row)
                next_id += 1
            except Exception as e:
                skipped_variants += 1
                continue

    print(f"\n📈 Extraction Summary:")
    print(f"  ✅ Total products processed: {len(products)}")
    print(f"  ⚠️ Skipped products: {skipped_products}")
    print(f"  ⚠️ Skipped variants: {skipped_variants}")
    print(f"  🔄 Variants with missing prices (set to 0.0): {null_price_variants}")
    print(f"  🎯 Total extracted rows: {len(rows)}")
    
    if not rows:
        print("❌ No rows to load.")
        return

    # Load to BigQuery
    print(f"\n💾 Loading {len(rows)} rows to BigQuery...")
    try:
        table_id = f"{PROJECT_ID}.{WAREHOUSE_DATASET}.{TABLE}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()
        print(f"✅ Successfully loaded {len(rows)} rows to {table_id}")
        
        # Show sample of loaded data
        print(f"\n📊 Sample loaded rows:")
        for i, row in enumerate(rows[:3]):
            print(f"  Row {i+1}: ID={row['price_fact_id']}, Variant={row['variant_id']}, Price={row['current_price']}, Available={row['is_available']}")
            
    except Exception as e:
        print(f"❌ Failed to load to BigQuery: {e}")
        print(f"💾 Saving to local file as backup...")
        
        # Save to local JSON file as backup
        backup_file = f"fact_product_price_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(rows, f, indent=2)
        print(f"✅ Saved {len(rows)} rows to {backup_file}")

if __name__ == "__main__":
    main()
