"""
JSON File Merger for LifeMobile Scraped Data

This script merges the output from 4 parallel scraping scripts into a single consolidated JSON file.
It also removes duplicates, provides statistics, creates a clean final dataset, and uploads to Azure Data Lake Storage.
After successful upload, all JSON files are deleted.
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Set
import logging
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables
load_dotenv()

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
        self.all_products = []
        self.all_json_files = []  # Track all JSON files for cleanup
        self.statistics = {
            "total_products_before_dedup": 0,
            "total_products_after_dedup": 0,
            "duplicates_removed": 0,
            "files_processed": 0,
            "files_found": 0,
            "products_per_script": {},
            "merge_timestamp": datetime.now().isoformat(),
        }

    def load_json_file(self, filename: str) -> List[Dict]:
        """Load products from a single JSON file"""
        if not os.path.exists(filename):
            logger.warning(f"File not found: {filename}")
            return []

        try:
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Handle different JSON structures
            if isinstance(data, list):
                products = data
            elif isinstance(data, dict) and "products" in data:
                products = data["products"]
            elif isinstance(data, dict):
                # If it's a single product object, wrap in list
                products = [data]
            else:
                logger.error(f"Unexpected JSON structure in {filename}")
                return []

            logger.info(f"Loaded {len(products)} products from {filename}")
            self.statistics["products_per_script"][filename] = len(products)
            self.statistics["files_found"] += 1
            return products

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {filename}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return []

    def deduplicate_products(self, products: List[Dict]) -> List[Dict]:
        """Remove duplicate products based on product_id_native and product_url"""
        seen_ids: Set[str] = set()
        seen_urls: Set[str] = set()
        unique_products = []

        for product in products:
            product_id = product.get("product_id_native", "")
            product_url = product.get("product_url", "")

            # Create a unique key for this product
            unique_key = f"{product_id}_{product_url}"

            if unique_key not in seen_ids and product_url not in seen_urls:
                unique_products.append(product)
                seen_ids.add(unique_key)
                seen_urls.add(product_url)
            else:
                self.statistics["duplicates_removed"] += 1

        return unique_products

    def enhance_product_data(self, product: Dict) -> Dict:
        """Add additional metadata and clean up product data"""
        # Ensure all required fields exist
        enhanced_product = {
            "product_id_native": product.get("product_id_native", ""),
            "product_url": product.get("product_url", ""),
            "product_title": product.get("product_title", ""),
            "description_html": product.get("description_html"),
            "brand": product.get("brand"),
            "category_path": product.get("category_path", []),
            "specifications": product.get("specifications", {}),
            "image_urls": product.get("image_urls", []),
            "variants": product.get("variants", []),
            "metadata": product.get("metadata", {}),
        }

        # Add merge metadata
        if "metadata" not in enhanced_product:
            enhanced_product["metadata"] = {}

        enhanced_product["metadata"]["merged_at"] = datetime.now().isoformat()
        enhanced_product["metadata"]["merger_version"] = "1.0"

        # Clean up empty fields
        if not enhanced_product["description_html"]:
            enhanced_product["description_html"] = None
        if not enhanced_product["brand"]:
            enhanced_product["brand"] = None

        return enhanced_product

    def generate_summary_statistics(self, products: List[Dict]) -> Dict:
        """Generate comprehensive statistics about the merged data"""
        stats = {
            "total_products": len(products),
            "brands": {},
            "categories": {},
            "price_ranges": {
                "with_price": 0,
                "without_price": 0,
                "min_price": float("inf"),
                "max_price": 0,
            },
            "image_statistics": {
                "products_with_images": 0,
                "products_without_images": 0,
                "total_images": 0,
                "avg_images_per_product": 0,
            },
            "variant_statistics": {
                "products_with_variants": 0,
                "total_variants": 0,
                "avg_variants_per_product": 0,
            },
        }

        total_images = 0
        total_variants = 0

        for product in products:
            # Brand statistics
            brand = product.get("brand", "Unknown")
            stats["brands"][brand] = stats["brands"].get(brand, 0) + 1

            # Category statistics
            categories = product.get("category_path", [])
            if categories:
                main_category = categories[0] if categories else "Uncategorized"
                stats["categories"][main_category] = (
                    stats["categories"].get(main_category, 0) + 1
                )

            # Price statistics
            variants = product.get("variants", [])
            has_price = False
            for variant in variants:
                price_str = variant.get("price_current", "0")
                if price_str and price_str != "0":
                    try:
                        price = float(price_str.replace(",", ""))
                        if price > 0:
                            has_price = True
                            stats["price_ranges"]["min_price"] = min(
                                stats["price_ranges"]["min_price"], price
                            )
                            stats["price_ranges"]["max_price"] = max(
                                stats["price_ranges"]["max_price"], price
                            )
                    except (ValueError, TypeError):
                        pass

            if has_price:
                stats["price_ranges"]["with_price"] += 1
            else:
                stats["price_ranges"]["without_price"] += 1

            # Image statistics
            images = product.get("image_urls", [])
            if images:
                stats["image_statistics"]["products_with_images"] += 1
                total_images += len(images)
            else:
                stats["image_statistics"]["products_without_images"] += 1

            # Variant statistics
            if len(variants) > 1:
                stats["variant_statistics"]["products_with_variants"] += 1
            total_variants += len(variants)

        # Calculate averages
        if len(products) > 0:
            stats["image_statistics"]["total_images"] = total_images
            stats["image_statistics"]["avg_images_per_product"] = round(
                total_images / len(products), 2
            )
            stats["variant_statistics"]["total_variants"] = total_variants
            stats["variant_statistics"]["avg_variants_per_product"] = round(
                total_variants / len(products), 2
            )

        # Fix infinite min_price
        if stats["price_ranges"]["min_price"] == float("inf"):
            stats["price_ranges"]["min_price"] = 0

        return stats

    def upload_to_adls(self, json_data: str, source_website: str) -> bool:
        """
        Uploads merged JSON data to Azure Data Lake Storage

        Args:
            json_data: Ready-to-upload JSON string with properly serialized data
            source_website: Name of the source website (used for partitioning)

        Returns:
            bool: True if upload successful, False otherwise
        """
        # Get Azure Connection String from Environment Variable
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            logger.error("Azure connection string not found in environment variables.")
            return False

        # Define the partitioned path
        scrape_date = datetime.now().strftime("%Y-%m-%d")
        file_path = (
            f"source_website={source_website}/scrape_date={scrape_date}/data.json"
        )
        container_name = "raw-data"

        try:
            # Connect to Azure and Upload
            blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
            blob_client = blob_service_client.get_blob_client(
                container=container_name, blob=file_path
            )

            logger.info(f"☁️ Uploading data to: {container_name}/{file_path}")

            # Upload the JSON string
            blob_client.upload_blob(json_data, overwrite=True)

            logger.info("✅ Upload to Azure Data Lake Storage completed successfully!")
            logger.info(f"📍 Location: {container_name}/{file_path}")
            return True

        except Exception as e:
            logger.error(f"❌ ADLS upload error: {e}", exc_info=True)
            return False

    def cleanup_json_files(self) -> None:
        """Delete all JSON files and completion markers after successful upload"""
        files_to_delete = self.input_files + [self.output_file]

        # Also check for any other lifemobile JSON files
        for file in os.listdir("."):
            if file.startswith("lifemobile_products_") and file.endswith(".json"):
                if file not in files_to_delete:
                    files_to_delete.append(file)

        # Add completion marker files
        completion_markers = [
            "script1.complete",
            "script2.complete",
            "script3.complete",
            "script4.complete",
        ]
        files_to_delete.extend(completion_markers)

        deleted_count = 0
        for file_path in files_to_delete:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.info(f"🗑️ Deleted: {file_path}")
                    deleted_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Could not delete {file_path}: {e}")

        if deleted_count > 0:
            logger.info(f"✅ Cleanup complete: {deleted_count} files deleted")
        else:
            logger.info("📂 No files to clean up")

    def merge_files(self) -> bool:
        """Main method to merge all JSON files"""
        logger.info("Starting LifeMobile data merge process...")

        # Load all files
        all_products = []
        for filename in self.input_files:
            products = self.load_json_file(filename)
            all_products.extend(products)
            self.statistics["files_processed"] += 1

        if not all_products:
            logger.error("No products found in any input files!")
            return False

        self.statistics["total_products_before_dedup"] = len(all_products)
        logger.info(f"Total products loaded: {len(all_products)}")

        # Remove duplicates
        logger.info("Removing duplicates...")
        unique_products = self.deduplicate_products(all_products)
        self.statistics["total_products_after_dedup"] = len(unique_products)

        logger.info(f"Products after deduplication: {len(unique_products)}")
        logger.info(f"Duplicates removed: {self.statistics['duplicates_removed']}")

        # Enhance product data
        logger.info("Enhancing product data...")
        enhanced_products = [
            self.enhance_product_data(product) for product in unique_products
        ]

        # Generate statistics
        logger.info("Generating statistics...")
        summary_stats = self.generate_summary_statistics(enhanced_products)

        # Create final output structure
        final_output = {
            "metadata": {
                "source_website": "lifemobile.lk",
                "merge_timestamp": self.statistics["merge_timestamp"],
                "total_products": len(enhanced_products),
                "scraping_method": "Scrapy parallel execution",
                "merger_version": "1.0",
                "input_files": self.input_files,
                "merge_statistics": self.statistics,
            },
            "summary_statistics": summary_stats,
            "products": enhanced_products,
        }

        # Save merged file
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(final_output, f, indent=2, ensure_ascii=False)

            logger.info(f"✅ Merged data saved to: {self.output_file}")
            logger.info(f"Final product count: {len(enhanced_products)}")
            return True

        except Exception as e:
            logger.error(f"Error saving merged file: {e}")
            return False

    def print_summary(self):
        """Print a summary of the merge operation"""
        print("\n" + "=" * 60)
        print("LIFEMOBILE DATA MERGE SUMMARY")
        print("=" * 60)
        print(f"Input files processed: {self.statistics['files_processed']}")
        print(f"Input files found: {self.statistics['files_found']}")
        print(
            f"Total products before dedup: {self.statistics['total_products_before_dedup']}"
        )
        print(
            f"Total products after dedup: {self.statistics['total_products_after_dedup']}"
        )
        print(f"Duplicates removed: {self.statistics['duplicates_removed']}")
        print(f"Output file: {self.output_file}")
        print("\nProducts per script:")
        for filename, count in self.statistics["products_per_script"].items():
            print(f"  {filename}: {count} products")
        print("=" * 60)


def main():
    """Main execution function"""
    merger = LifeMobileDataMerger()

    try:
        # Step 1: Merge all JSON files
        logger.info("=" * 60)
        logger.info("STEP 1: MERGING JSON FILES")
        logger.info("=" * 60)
        success = merger.merge_files()
        merger.print_summary()

        if not success:
            logger.error("❌ Merge failed!")
            return 1

        logger.info("✅ Merge completed successfully!")

        # Step 2: Upload to Azure Data Lake Storage
        logger.info("\n" + "=" * 60)
        logger.info("STEP 2: UPLOADING TO AZURE DATA LAKE STORAGE")
        logger.info("=" * 60)

        try:
            # Read the merged file
            with open(merger.output_file, "r", encoding="utf-8") as f:
                merged_data = json.load(f)

            # Convert to JSON string
            json_data = json.dumps(merged_data, indent=2, ensure_ascii=False)

            # Validate JSON
            try:
                json.loads(json_data)
                logger.info("✅ JSON validation passed")
            except json.JSONDecodeError as e:
                logger.error(f"❌ JSON validation failed: {e}")
                return 1

            # Upload to ADLS
            upload_success = merger.upload_to_adls(
                json_data=json_data, source_website="lifemobile.lk"
            )

            if upload_success:
                logger.info(
                    "\n🎉 Data successfully uploaded to Azure Data Lake Storage!"
                )
                logger.info(f"📊 Final Summary:")
                logger.info(
                    f"   📦 Products uploaded: {merged_data['metadata']['total_products']:,}"
                )
                logger.info(f"   💾 Data size: {len(json_data) / (1024*1024):.2f} MB")

                # Step 3: Cleanup all JSON files
                logger.info("\n" + "=" * 60)
                logger.info("STEP 3: CLEANING UP JSON FILES")
                logger.info("=" * 60)
                merger.cleanup_json_files()

                logger.info("\n" + "=" * 60)
                logger.info("✅ ALL STEPS COMPLETED SUCCESSFULLY!")
                logger.info("=" * 60)
                return 0
            else:
                logger.error("❌ Failed to upload data to ADLS")
                logger.warning("⚠️ JSON files NOT deleted due to upload failure")
                return 1

        except FileNotFoundError:
            logger.error(f"❌ Merged file not found: {merger.output_file}")
            return 1
        except Exception as e:
            logger.error(f"❌ Failed to upload to ADLS: {e}", exc_info=True)
            logger.warning("⚠️ JSON files NOT deleted due to upload failure")
            return 1

    except KeyboardInterrupt:
        logger.info("Merge interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    main()
