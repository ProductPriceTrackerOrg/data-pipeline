"""
Onei.lk Web Scraper
Scrapes product data from Onei.lk and uploads as JSON to Azure Data Lake Storage (ADLS)

Usage:
    python main.py
"""

import logging
import sys
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the current directory to the path to allow imports
sys.path.append(".")

# Azure Storage imports
from azure.storage.blob import BlobServiceClient

from scripts.product_scraper_manager import run_scraper


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                      Onei.lk Web Scraper                    ║
║                  Simple Product Data Scraper                 ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def upload_to_adls(json_data: str, source_website: str):
    """
    Uploads a JSON string to ADLS as a single JSON file.

    Args:
        json_data: Ready-to-upload JSON string with properly serialized data
        source_website: Name of the source website (used for partitioning)
    """
    # --- 1. Get Azure Connection String from Environment Variable ---
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("Azure connection string not found in environment variables.")

    # --- 2. Define the partitioned path ---
    scrape_date = datetime.now().strftime("%Y-%m-%d")
    file_path = f"source_website={source_website}/scrape_date={scrape_date}/data.json"
    container_name = "raw-data"

    try:
        # --- 3. Connect to Azure and Upload ---
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=file_path
        )

        print(f"Uploading data to: {container_name}/{file_path}")

        # Upload the already prepared JSON string
        blob_client.upload_blob(json_data, overwrite=True)

        print("Upload to ADLS successful!")
        print(f"Blob URL: {blob_client.url}")
        return True

    except Exception as e:
        print(f"ADLS upload error: {e}")
        logging.error(f"ADLS upload error: {e}", exc_info=True)
        return False


def fix_malformed_json(json_path="one1lk_products.json"):
    """
    Fix malformed JSON by reading as JSONL (one JSON object per line)
    """
    products = []
    line_count = 0
    error_count = 0

    print(f"🔧 Attempting to fix malformed JSON file: {json_path}")

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Try to parse as regular JSON first
        try:
            products = json.loads(content)
            print("✅ JSON is valid, no fixing needed")
            return products
        except json.JSONDecodeError:
            print("❌ JSON is malformed, attempting line-by-line parsing...")

        # Split by lines and parse each line as JSON
        lines = content.strip().split("\n")
        for i, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            # Remove trailing comma if present
            if line.endswith(","):
                line = line[:-1]

            # Skip lines that are just brackets
            if line in ["{", "}", "[", "]"]:
                continue

            try:
                # Try to parse the line as JSON
                product = json.loads(line)
                products.append(product)
                line_count += 1
            except json.JSONDecodeError as e:
                error_count += 1
                print(f"⚠️  Error parsing line {i}: {str(e)[:100]}...")
                continue

        print(f"✅ Successfully parsed {line_count} products, {error_count} errors")
        return products

    except Exception as e:
        print(f"❌ Failed to fix JSON: {e}")
        return []


def load_scraped_products(json_path="one1lk_products.json"):
    """
    Load scraped products from the local JSON file with error handling
    """
    if not os.path.exists(json_path):
        raise FileNotFoundError(
            f"{json_path} not found. Scraper did not produce output."
        )

    try:
        # First try normal JSON loading
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        print(f"❌ JSON parsing error: {e}")
        print("🔧 Attempting to fix malformed JSON...")

        # Try to fix malformed JSON
        fixed_data = fix_malformed_json(json_path)
        if fixed_data:
            # Save the fixed data
            backup_path = json_path.replace(".json", "_fixed.json")
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(fixed_data, f, indent=2, ensure_ascii=False)
            print(f"✅ Fixed data saved to: {backup_path}")
            return fixed_data
        else:
            raise Exception("Unable to fix malformed JSON file")


def main():
    """Main entry point - scrape all products and save as JSON"""
    print_banner()
    print("Starting Onei.lk product scraping...")
    print(f"Scraping started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler("onei_scraper.log"), logging.StreamHandler()],
        )

        # Run scraping using the existing scraper
        print("\n🕷️  Scraping all products from Onei.lk...")
        run_scraper()

        # Load scraped products
        print("📂 Loading scraped data...")
        products_data = load_scraped_products("one1lk_products.json")

        if not products_data:
            print("❌ No valid products found in scraped data")
            return 1

        # Calculate totals
        total_products = len(products_data)
        total_variants = sum(
            len(product.get("variants", [])) for product in products_data
        )

        # Display results
        print(f"\n{'='*60}")
        print(" SCRAPING COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f" Products scraped: {total_products:,}")
        print(f" Variants scraped: {total_variants:,}")

        # Upload data to Azure Data Lake Storage
        try:
            print(f"\n{'='*60}")
            print(" UPLOADING TO AZURE DATA LAKE STORAGE")
            print(f"{'='*60}")

            # Handle datetime serialization
            def datetime_handler(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

            # Convert to JSON string with proper datetime handling
            json_data = json.dumps(
                products_data, indent=2, default=datetime_handler, ensure_ascii=False
            )

            # Validate JSON before upload
            try:
                json.loads(json_data)
                print("✅ JSON validation passed")
            except json.JSONDecodeError as e:
                print(f"❌ JSON validation failed: {e}")
                return 1

            # Upload to ADLS
            upload_success = upload_to_adls(
                json_data=json_data, source_website="onei.lk"
            )

            if upload_success:
                print("🎉 Data successfully uploaded to Azure Data Lake Storage!")
            else:
                print("❌ Failed to upload data to ADLS")

        except Exception as e:
            print(f"❌ Failed to upload to ADLS: {e}")
            logging.error(f"ADLS upload error: {e}", exc_info=True)

        return 0

    except KeyboardInterrupt:
        print("\n⚠️  Scraping interrupted by user.")
        return 1
    except Exception as e:
        print(f"\n❌ Scraping failed: {e}")
        logging.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()  # Removed asyncio.run()
        exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nOperation interrupted by user.")
        exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)
