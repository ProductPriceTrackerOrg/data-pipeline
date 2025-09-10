from fact_product_price import find_latest_extracted_file, parse_price
import json

# Quick test to count how many variants would be processed with NULL prices
json_file = find_latest_extracted_file()
if json_file:
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    products = data if isinstance(data, list) else data.get("data", [])
    
    total_variants = 0
    variants_with_price = 0
    variants_with_null_price = 0
    variants_no_id = 0
    
    for i, product in enumerate(products):
        if isinstance(product, dict):
            variants = product.get("variants", [])
            for variant in variants:
                if isinstance(variant, dict):
                    total_variants += 1
                    variant_id_native = variant.get("variant_id_native")
                    price_current = variant.get("price_current")
                    
                    if not variant_id_native:
                        variants_no_id += 1
                    elif parse_price(price_current) is not None:
                        variants_with_price += 1
                    else:
                        variants_with_null_price += 1
    
    print(f"Variant Analysis:")
    print(f"  Total variants: {total_variants}")
    print(f"  Variants with valid prices: {variants_with_price}")
    print(f"  Variants with NULL prices: {variants_with_null_price}")
    print(f"  Variants without variant_id: {variants_no_id}")
    print(f"  Total processable variants: {variants_with_price + variants_with_null_price}")
else:
    print("No JSON file found")
