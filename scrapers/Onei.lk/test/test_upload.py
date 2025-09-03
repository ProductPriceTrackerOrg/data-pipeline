"""
Test Azure upload with existing data
"""

import sys
import os

sys.path.append(".")

from main import load_scraped_products, upload_to_adls
from datetime import datetime
import json


def test_upload():
    """Test uploading existing data to Azure"""
    try:
        print("📂 Loading scraped data...")
        products_data = load_scraped_products("one1lk_products.json")

        if not products_data:
            print("❌ No data found")
            return False

        print(f"✅ Loaded {len(products_data)} products")

        # Handle datetime serialization
        def datetime_handler(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

        # Convert to JSON string
        print("🔄 Converting to JSON...")
        json_data = json.dumps(
            products_data, indent=2, default=datetime_handler, ensure_ascii=False
        )

        # Validate JSON
        print("✅ JSON validation...")
        json.loads(json_data)  # This will raise an error if invalid
        print("✅ JSON is valid")

        # Upload to Azure
        print("☁️  Uploading to Azure Data Lake Storage...")
        upload_success = upload_to_adls(json_data=json_data, source_website="onei.lk")

        if upload_success:
            print("🎉 Upload successful!")
            return True
        else:
            print("❌ Upload failed")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    print("🚀 Testing Azure Upload")
    print("=" * 50)
    test_upload()
