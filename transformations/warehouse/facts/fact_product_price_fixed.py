"""
FactProductPrice Loader Script - FIXED VERSION
Extracts daily price and availability for each product variant from extracted JSON and loads to BigQuery.
"""
import json
import hashlib
from datetime import datetime
import os

# Config
PROJECT_ID = "price-pulse-470211"
DATASET = "warehouse"
TABLE = "FactProductPrice"

def find_latest_extracted_file():
    """Find the most recent extracted data file"""
    temp_data_dir = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "extraction", "temp_data"
    )
    
    # Look for sample_download.json first (your actual file)
    sample_file = os.path.join(temp_data_dir, "sample_download.json")
    if os.path.exists(sample_file):
        print(f"Using sample_download.json: {sample_file}")
        return sample_file
    
    # Fallback to other files
    import glob
    patterns = [
        os.path.join(temp_data_dir, "extracted_data_*.json"),
        os.path.join(temp_data_dir, "*.json")
    ]
    
    files = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    
    if not files:
        print(f"No JSON files found in {temp_data_dir}")
        return None
    
    # Return the most recent file
    latest_file = max(files, key=os.path.getctime)
    print(f"Using latest file: {latest_file}")
    return latest_file

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

def main():
    print("Starting FactProductPrice extraction...")
    
    # Find the JSON file
    json_file = find_latest_extracted_file()
    if not json_file:
        print("ERROR: No extracted data file found.")
        return
    
    # Load JSON
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        print(f"✓ Loaded JSON file: {os.path.basename(json_file)}")
        print(f"✓ JSON type: {type(data)}")
        
        if isinstance(data, list):
            products = data
            print(f"✓ Found {len(products)} products in list format")
        elif isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                products = data["data"]
                print(f"✓ Found {len(products)} products in data.data format")
            else:
                print(f"ERROR: Invalid JSON structure. Available keys: {list(data.keys())}")
                return
        else:
            print("ERROR: Expected a list of products or dict with 'data' key.")
            return
            
    except Exception as e:
        print(f"ERROR: Failed to load JSON file: {e}")
        return

    # Show sample product structure
    if products:
        print(f"\n✓ Sample product structure:")
        sample = products[0]
        print(f"  Keys: {list(sample.keys())}")
        if 'metadata' in sample:
            print(f"  Metadata keys: {list(sample['metadata'].keys())}")
        if 'variants' in sample and sample['variants']:
            print(f"  Variant keys: {list(sample['variants'][0].keys())}")

    # Process products
    print(f"\n📊 Processing {len(products)} products...")
    rows = []
    next_id = 0  # Start from 0 for testing
    skipped_products = 0
    skipped_variants = 0
    
    for i, product in enumerate(products):
        if i % 500 == 0:  # Progress indicator
            print(f"  Processing product {i}...")
            
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

            if not variant_id_native or price_current is None:
                skipped_variants += 1
                continue

            try:
                variant_id = get_variant_id(source_website, variant_id_native)
                current_price = float(price_current)
                original_price = float(price_original) if price_original else None
                available = is_available(availability_text)

                row = {
                    "price_fact_id": next_id,
                    "variant_id": variant_id,
                    "date_id": date_id,
                    "current_price": current_price,
                    "original_price": original_price,
                    "is_available": available
                }
                rows.append(row)
                next_id += 1
            except Exception as e:
                skipped_variants += 1
                continue

    print(f"\n📈 Extraction Summary:")
    print(f"  ✓ Total products processed: {len(products)}")
    print(f"  ⚠ Skipped products: {skipped_products}")
    print(f"  ⚠ Skipped variants: {skipped_variants}")
    print(f"  🎯 Extracted {len(rows)} price fact rows")
    
    if rows:
        print(f"\n📋 Sample extracted rows:")
        for i, row in enumerate(rows[:3]):
            print(f"  Row {i}: price_fact_id={row['price_fact_id']}, variant_id={row['variant_id']}, price={row['current_price']}, available={row['is_available']}")
    
    if not rows:
        print("❌ No rows to load.")
        return

    # Try to load to BigQuery
    print(f"\n🔄 Attempting BigQuery load...")
    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=PROJECT_ID)
        print("✓ BigQuery client initialized")
        
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        job = client.load_table_from_json(rows, table_id, job_config=job_config)
        job.result()
        print(f"✅ Successfully loaded {len(rows)} rows to {table_id}")
        
    except Exception as e:
        print(f"❌ Failed to load to BigQuery: {e}")
        print("💾 Saving data to local file instead...")
        
        # Save to local JSON file as backup
        output_file = f"extracted_price_facts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(rows, f, indent=2)
        print(f"✅ Saved {len(rows)} rows to {output_file}")

if __name__ == "__main__":
    main()
