"""
Quick test of fact_product_price extraction with NULL prices allowed
"""
from fact_product_price import find_latest_extracted_file, parse_price, get_variant_id, get_date_id, is_available
import json

def test_extraction():
    print("Testing extraction with NULL prices allowed...")
    
    # Find JSON file
    json_file = find_latest_extracted_file()
    if not json_file:
        print("No JSON file found")
        return
    
    # Load JSON
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    products = data if isinstance(data, list) else data.get("data", [])
    print(f"Processing {len(products)} products...")
    
    rows = []
    skipped_variants = 0
    null_price_variants = 0
    
    for i, product in enumerate(products):
        if not isinstance(product, dict):
            continue
            
        metadata = product.get("metadata", {})
        variants = product.get("variants", [])
        source_website = metadata.get("source_website")
        scrape_timestamp = metadata.get("scrape_timestamp")

        if not source_website or not scrape_timestamp:
            continue

        try:
            date_id = get_date_id(scrape_timestamp)
        except:
            continue

        for j, variant in enumerate(variants):
            if not isinstance(variant, dict):
                continue
                
            variant_id_native = variant.get("variant_id_native")
            price_current = variant.get("price_current")

            if not variant_id_native:
                skipped_variants += 1
                continue

            try:
                variant_id = get_variant_id(source_website, variant_id_native)
                current_price = parse_price(price_current)
                
                if current_price is None:
                    null_price_variants += 1
                
                row = {
                    "variant_id": variant_id,
                    "current_price": current_price,  # Can be None
                }
                rows.append(row)
            except:
                skipped_variants += 1

    print(f"\nResults:")
    print(f"  Total rows extracted: {len(rows)}")
    print(f"  Rows with NULL prices: {null_price_variants}")
    print(f"  Rows with valid prices: {len(rows) - null_price_variants}")
    print(f"  Skipped variants: {skipped_variants}")

if __name__ == "__main__":
    test_extraction()
