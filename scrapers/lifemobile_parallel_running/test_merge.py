"""
Test version of JSON File Merger - Shows output without deleting files or uploading to Azure
"""

import json
import os
import time
from datetime import datetime
from typing import List, Dict, Set
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LifeMobileDataMerger:
    def __init__(self):
        self.input_files = [
            "lifemobile_products_script1.json",
            "lifemobile_products_script2.json",
            "lifemobile_products_script3.json",
            "lifemobile_products_script4.json",
        ]
        self.output_file = "lifemobile_products_merged.json"
        self.merged_data = []
        self.stats = {
            "input_files_processed": 0,
            "input_files_found": 0,
            "total_products_before_dedup": 0,
            "total_products_after_dedup": 0,
            "duplicates_removed": 0,
            "products_per_script": {},
        }

    def load_json_files(self) -> List[Dict]:
        """Load and combine all JSON files."""
        all_products = []

        for file_path in self.input_files:
            self.stats["input_files_processed"] += 1

            if not os.path.exists(file_path):
                logger.warning(f"File not found: {file_path}")
                continue

            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    data = json.load(file)

                    # Handle both list and single object formats
                    if isinstance(data, list):
                        products = data
                    else:
                        products = [data]

                    all_products.extend(products)
                    self.stats["input_files_found"] += 1
                    self.stats["products_per_script"][file_path] = len(products)
                    logger.info(f"Loaded {len(products)} products from {file_path}")

            except Exception as e:
                logger.error(f"Error loading {file_path}: {e}")
                continue

        self.stats["total_products_before_dedup"] = len(all_products)
        logger.info(f"Total products loaded: {len(all_products)}")
        return all_products

    def remove_duplicates(self, products: List[Dict]) -> List[Dict]:
        """Remove duplicate products based on product_id or name."""
        seen_ids: Set[str] = set()
        seen_names: Set[str] = set()
        unique_products = []

        logger.info("Removing duplicates...")

        for product in products:
            # Use product_id as primary key, fallback to name
            product_id = product.get("product_id", "")
            product_name = product.get("name", "").lower().strip()

            # Skip if we've seen this product_id or name before
            if product_id and product_id in seen_ids:
                continue
            if product_name and product_name in seen_names:
                continue

            # Add to seen sets and unique products
            if product_id:
                seen_ids.add(product_id)
            if product_name:
                seen_names.add(product_name)
            unique_products.append(product)

        duplicates_removed = len(products) - len(unique_products)
        self.stats["total_products_after_dedup"] = len(unique_products)
        self.stats["duplicates_removed"] = duplicates_removed

        logger.info(f"Products after deduplication: {len(unique_products)}")
        logger.info(f"Duplicates removed: {duplicates_removed}")

        return unique_products

    def enhance_product_data(self, products: List[Dict]) -> List[Dict]:
        """Clean and enhance product data - keep only essential product information."""
        logger.info("Enhancing product data...")

        enhanced_products = []
        for product in products:
            # Create clean product object with only essential data
            clean_product = {
                "product_id": product.get("product_id", ""),
                "name": product.get("name", ""),
                "price": product.get("price", ""),
                "brand": product.get("brand", ""),
                "category": product.get("category", ""),
                "availability": product.get("availability", ""),
                "image_url": product.get("image_url", ""),
                "specifications": product.get("specifications", {}),
                "url": product.get("url", ""),
            }

            # Remove empty fields
            clean_product = {k: v for k, v in clean_product.items() if v}
            enhanced_products.append(clean_product)

        return enhanced_products

    def save_merged_data(self, products: List[Dict]) -> bool:
        """Save merged data to JSON file with clean structure."""
        try:
            logger.info("Generating statistics...")

            # Create clean JSON output - just the product array
            with open(self.output_file, "w", encoding="utf-8") as file:
                json.dump(products, file, indent=2, ensure_ascii=False)

            logger.info(f"✅ Merged data saved to: {self.output_file}")
            logger.info(f"Final product count: {len(products)}")
            return True

        except Exception as e:
            logger.error(f"❌ Error saving merged data: {e}")
            return False

    def print_summary(self):
        """Print detailed summary of the merge process."""
        print("\n" + "=" * 60)
        print("LIFEMOBILE DATA MERGE SUMMARY")
        print("=" * 60)
        print(f"Input files processed: {self.stats['input_files_processed']}")
        print(f"Input files found: {self.stats['input_files_found']}")
        print(
            f"Total products before dedup: {self.stats['total_products_before_dedup']}"
        )
        print(f"Total products after dedup: {self.stats['total_products_after_dedup']}")
        print(f"Duplicates removed: {self.stats['duplicates_removed']}")
        print(f"Output file: {self.output_file}")
        print(f"\nProducts per script:")
        for script, count in self.stats["products_per_script"].items():
            print(f"  {script}: {count} products")
        print("=" * 60)

    def run(self) -> bool:
        """Main execution method."""
        logger.info("=" * 60)
        logger.info("Starting LifeMobile data merge process...")

        # Step 1: Load all JSON files
        all_products = self.load_json_files()

        if not all_products:
            logger.error("No products found in any input files!")
            self.print_summary()
            logger.error("❌ Merge failed!")
            return False

        # Step 2: Remove duplicates
        unique_products = self.remove_duplicates(all_products)

        # Step 3: Enhance product data
        enhanced_products = self.enhance_product_data(unique_products)

        # Step 4: Save merged data
        success = self.save_merged_data(enhanced_products)

        # Step 5: Print summary
        self.print_summary()

        if success:
            logger.info("✅ Merge completed successfully!")
            return True
        else:
            logger.error("❌ Merge failed!")
            return False


def main():
    """Main function."""
    merger = LifeMobileDataMerger()
    success = merger.run()

    if success:
        print(f"\n🎉 SUCCESS! Merged data saved to '{merger.output_file}'")
        print(
            "📝 Note: This is a test run - no files were deleted and no upload to Azure was performed."
        )
    else:
        print("❌ FAILED! Check the logs above for details.")


if __name__ == "__main__":
    main()
