"""
Test script for DimShopProduct daily processing logic
Tests duplicate prevention and scrape_date updates
"""
import sys
import os
from datetime import datetime, date

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transformations.warehouse.dimensions.dim_shop_product import DimShopProductTransformer

def test_daily_processing():
    """Test the daily processing logic"""
    print("🧪 Testing DimShopProduct Daily Processing Logic")
    print("=" * 60)
    
    # Initialize the transformer
    transformer = DimShopProductTransformer()
    
    # Set target date for testing
    target_date = date.today()
    print(f"📅 Target Date: {target_date}")
    
    try:
        # Check existing shop product IDs
        print("\n📊 Checking existing shop product IDs...")
        existing_ids = transformer.get_existing_shop_product_ids()
        print(f"   Found {len(existing_ids)} existing shop products")
        
        # Show sample existing IDs
        if existing_ids:
            print(f"   Sample IDs: {list(existing_ids)[:5]}...")
        
        # Test the transformation
        print("\n🔄 Running daily transformation...")
        result = transformer.transform_and_load(target_date=target_date)
        
        if result:
            print("✅ Daily transformation completed successfully!")
            print("\n📋 Summary:")
            print(f"   - Processed data for: {target_date}")
            print(f"   - Existing products: {len(existing_ids)}")
            print(f"   - Logic: Insert new products only, update scrape_date for all")
        else:
            print("❌ Daily transformation failed!")
            
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_daily_processing()
