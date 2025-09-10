import json
import os

def find_latest_extracted_file():
    """Find the most recent extracted data file"""
    temp_data_dir = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "extraction", "temp_data"
    )
    
    print(f"Looking in directory: {temp_data_dir}")
    
    # Look for sample_download.json first (your actual file)
    sample_file = os.path.join(temp_data_dir, "sample_download.json")
    if os.path.exists(sample_file):
        print(f"Found sample_download.json: {sample_file}")
        return sample_file
    
    print("sample_download.json not found")
    return None

def main():
    print("Starting fact_product_price extraction test...")
    
    # Find the JSON file
    json_file = find_latest_extracted_file()
    if not json_file:
        print("No extracted data file found.")
        return
    
    # Load JSON
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        print(f"Loaded JSON file: {json_file}")
        print(f"JSON type: {type(data)}")
        
        if isinstance(data, list):
            products = data
            print(f"Found {len(products)} products in list format")
        else:
            print("Expected a list of products")
            return
            
    except Exception as e:
        print(f"Failed to load JSON file: {e}")
        return

    # Test first few products
    rows = []
    for i, product in enumerate(products[:5]):  # Test first 5 products only
        if not isinstance(product, dict):
            print(f"Product {i}: not a dictionary")
            continue
            
        metadata = product.get("metadata", {})
        variants = product.get("variants", [])
        source_website = metadata.get("source_website")
        scrape_timestamp = metadata.get("scrape_timestamp")

        print(f"Product {i}: source={source_website}, timestamp={scrape_timestamp}, variants={len(variants)}")

        if not source_website or not scrape_timestamp:
            print(f"  Skipping: missing metadata")
            continue

        for j, variant in enumerate(variants):
            if not isinstance(variant, dict):
                print(f"  Variant {j}: not a dictionary")
                continue
                
            variant_id_native = variant.get("variant_id_native")
            price_current = variant.get("price_current")
            availability_text = variant.get("availability_text", "")

            print(f"  Variant {j}: id={variant_id_native}, price={price_current}, availability={availability_text}")

            if not variant_id_native or price_current is None:
                print(f"    Skipping: missing required fields")
                continue

            try:
                current_price = float(price_current)
                print(f"    Success: price={current_price}")
                rows.append({"variant_id": variant_id_native, "price": current_price})
            except Exception as e:
                print(f"    Error processing: {e}")
                continue

    print(f"\nTotal processed rows: {len(rows)}")
    for row in rows[:3]:  # Show first 3 rows
        print(f"  {row}")

if __name__ == "__main__":
    main()
