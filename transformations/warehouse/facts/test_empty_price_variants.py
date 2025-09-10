"""
Quick test to find specific examples of skipped variants
"""
import json

# Load the JSON file
json_file = r"D:\Semester 5\DSE Project\data-pipeline\transformations\extraction\temp_data\extracted_data_2025-09-03_100700.json"

with open(json_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Handle nested structure
if isinstance(data, dict) and "data" in data:
    all_products = []
    for website, website_products in data["data"].items():
        if isinstance(website_products, list):
            all_products.extend(website_products)
    products = all_products
else:
    products = data

print(f"Checking {len(products)} products for skipped variants...")

# Find variants with empty prices
empty_price_examples = []
count = 0

for i, product in enumerate(products):
    if isinstance(product, dict):
        variants = product.get("variants", [])
        for j, variant in enumerate(variants):
            if isinstance(variant, dict):
                price_current = variant.get("price_current")
                variant_id_native = variant.get("variant_id_native")
                
                # Check for empty prices (these would be skipped previously)
                if variant_id_native and (price_current == '' or price_current is None):
                    empty_price_examples.append({
                        "product_index": i,
                        "variant_index": j,
                        "product_id": product.get("product_id_native"),
                        "product_title": product.get("product_title"),
                        "variant_id_native": variant_id_native,
                        "price_current": price_current,
                        "price_original": variant.get("price_original"),
                        "availability_text": variant.get("availability_text"),
                        "full_variant": variant
                    })
                    count += 1
                    
                    if count >= 10:  # Show first 10 examples
                        break
        if count >= 10:
            break

print(f"\nFound {len(empty_price_examples)} examples of variants with empty prices:")
print("="*80)

for i, example in enumerate(empty_price_examples):
    print(f"\nExample {i+1}:")
    print(f"  Product: {example['product_title']}")
    print(f"  Product ID: {example['product_id']}")
    print(f"  Variant ID: {example['variant_id_native']}")
    print(f"  Price Current: '{example['price_current']}'")
    print(f"  Price Original: '{example['price_original']}'")
    print(f"  Availability: '{example['availability_text']}'")
    print(f"  Full Variant JSON:")
    print(json.dumps(example['full_variant'], indent=4))
    print("-" * 80)

# Save to file for detailed inspection
with open("empty_price_variants_examples.json", "w", encoding="utf-8") as f:
    json.dump(empty_price_examples, f, indent=2, ensure_ascii=False)

print(f"\n💾 Saved examples to empty_price_variants_examples.json")
