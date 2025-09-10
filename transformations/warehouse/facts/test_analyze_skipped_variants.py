"""
Test case to examine skipped variants and understand their JSON structure
"""
import json
import os
from fact_product_price import parse_price, get_variant_id, get_date_id, is_available

def analyze_skipped_variants():
    """
    Analyze variants that are being skipped and extract their JSON objects
    """
    # Load the JSON file
    json_file = r"D:\Semester 5\DSE Project\data-pipeline\transformations\extraction\temp_data\extracted_data_2025-09-03_100700.json"
    
    if not os.path.exists(json_file):
        print(f"JSON file not found: {json_file}")
        return
    
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Handle different JSON structures
    if isinstance(data, dict):
        if "data" in data and isinstance(data["data"], dict):
            # Nested structure like {"data": {"appleme": [...], "simplytek": [...]}}
            all_products = []
            for website, website_products in data["data"].items():
                if isinstance(website_products, list):
                    all_products.extend(website_products)
            products = all_products
        elif "data" in data and isinstance(data["data"], list):
            products = data["data"]
        else:
            print("Unexpected JSON structure")
            return
    else:
        products = data
    
    print(f"Analyzing {len(products)} products...")
    
    # Categories of skipped variants
    skipped_categories = {
        "missing_variant_id": [],
        "missing_metadata": [],
        "invalid_timestamp": [],
        "empty_price": [],
        "unparseable_price": [],
        "processing_error": []
    }
    
    processed_variants = []
    
    for i, product in enumerate(products):
        if not isinstance(product, dict):
            continue
            
        metadata = product.get("metadata", {})
        variants = product.get("variants", [])
        source_website = metadata.get("source_website")
        scrape_timestamp = metadata.get("scrape_timestamp")

        # Check metadata issues
        if not source_website or not scrape_timestamp:
            skipped_categories["missing_metadata"].append({
                "product_index": i,
                "product_id": product.get("product_id_native"),
                "metadata": metadata,
                "reason": f"Missing source_website: {source_website}, scrape_timestamp: {scrape_timestamp}"
            })
            continue

        # Check timestamp parsing
        try:
            date_id = get_date_id(scrape_timestamp)
        except Exception as e:
            skipped_categories["invalid_timestamp"].append({
                "product_index": i,
                "product_id": product.get("product_id_native"),
                "scrape_timestamp": scrape_timestamp,
                "error": str(e)
            })
            continue

        # Analyze each variant
        for j, variant in enumerate(variants):
            if not isinstance(variant, dict):
                continue
                
            variant_id_native = variant.get("variant_id_native")
            price_current = variant.get("price_current")
            price_original = variant.get("price_original")
            availability_text = variant.get("availability_text", "")

            # Check variant ID
            if not variant_id_native:
                skipped_categories["missing_variant_id"].append({
                    "product_index": i,
                    "variant_index": j,
                    "product_id": product.get("product_id_native"),
                    "variant": variant,
                    "reason": "Missing variant_id_native"
                })
                continue

            # Check price parsing
            try:
                variant_id = get_variant_id(source_website, variant_id_native)
                current_price = parse_price(price_current)
                original_price = parse_price(price_original) if price_original else None
                
                if price_current == '' or price_current is None:
                    skipped_categories["empty_price"].append({
                        "product_index": i,
                        "variant_index": j,
                        "product_id": product.get("product_id_native"),
                        "variant_id_native": variant_id_native,
                        "price_current": price_current,
                        "variant": variant
                    })
                elif current_price is None and price_current:
                    skipped_categories["unparseable_price"].append({
                        "product_index": i,
                        "variant_index": j,
                        "product_id": product.get("product_id_native"),
                        "variant_id_native": variant_id_native,
                        "price_current": price_current,
                        "variant": variant
                    })
                else:
                    # Successfully processed
                    processed_variants.append({
                        "product_index": i,
                        "variant_index": j,
                        "product_id": product.get("product_id_native"),
                        "variant_id_native": variant_id_native,
                        "current_price": current_price,
                        "original_price": original_price,
                        "available": is_available(availability_text)
                    })
                    
            except Exception as e:
                skipped_categories["processing_error"].append({
                    "product_index": i,
                    "variant_index": j,
                    "product_id": product.get("product_id_native"),
                    "variant_id_native": variant_id_native,
                    "error": str(e),
                    "variant": variant
                })

    # Print analysis results
    print("\n" + "="*60)
    print("VARIANT ANALYSIS RESULTS")
    print("="*60)
    
    print(f"\n✅ Successfully processed variants: {len(processed_variants)}")
    
    for category, items in skipped_categories.items():
        if items:
            print(f"\n❌ {category.replace('_', ' ').title()}: {len(items)} variants")
            
            # Show first few examples
            for i, item in enumerate(items[:3]):
                print(f"   Example {i+1}:")
                if 'variant' in item:
                    print(f"     Product: {item.get('product_id', 'Unknown')}")
                    print(f"     Variant: {json.dumps(item['variant'], indent=6)}")
                else:
                    print(f"     {json.dumps(item, indent=6)}")
                print()
            
            if len(items) > 3:
                print(f"     ... and {len(items) - 3} more")
    
    # Save detailed results to files
    for category, items in skipped_categories.items():
        if items:
            filename = f"skipped_variants_{category}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(items, f, indent=2, ensure_ascii=False)
            print(f"\n💾 Saved detailed {category} analysis to {filename}")
    
    # Save processed variants sample
    if processed_variants:
        with open("processed_variants_sample.json", 'w', encoding='utf-8') as f:
            json.dump(processed_variants[:100], f, indent=2, ensure_ascii=False)
        print(f"💾 Saved sample of processed variants to processed_variants_sample.json")

if __name__ == "__main__":
    analyze_skipped_variants()
