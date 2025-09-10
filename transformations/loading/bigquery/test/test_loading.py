#!/usr/bin/env python3
"""
Test BigQuery loading functionality
"""
import os
import json
import logging
from datetime import datetime, timedelta
from loader import BigQueryLoader
from transformations.loading.bigquery.staging_data_cleaner import get_staging_table_name

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_dataset_creation():
    """Test dataset creation and access"""
    logger.info("🧪 Test 1: Dataset Creation")
    
    try:
        loader = BigQueryLoader()
        logger.info("✅ Dataset creation/access successful")
        return True
    except Exception as e:
        logger.error(f"❌ Dataset creation failed: {str(e)}")
        return False

def test_table_creation():
    """Test staging table creation"""
    logger.info("🧪 Test 2: Table Creation")
    
    try:
        loader = BigQueryLoader()
        
        # Test table creation for multiple sources
        test_sources = ["test_appleme", "test_cyberdeals", "test_simplytek"]
        
        for source in test_sources:
            table_id = loader.ensure_staging_table_exists(source)
            table_name = get_staging_table_name(source)
            logger.info(f"✅ Created/verified table: {table_name}")
        
        logger.info("✅ Table creation test successful")
        return True
        
    except Exception as e:
        logger.error(f"❌ Table creation failed: {str(e)}")
        return False

def test_data_loading():
    """Test loading sample data"""
    logger.info("🧪 Test 3: Data Loading")
    
    try:
        loader = BigQueryLoader()
        
        # Create sample data that mimics real product structure
        sample_products = [
            {
                "product_id_native": "TEST_001",
                "product_url": "https://test.lk/product/test-product-1",
                "product_title": "Test Product 1 - Sample Item",
                "description_html": "<p>Test product description</p>",
                "brand": "Test Brand",
                "category_path": ["Electronics", "Test Category"],
                "image_urls": ["https://test.lk/image1.jpg"],
                "variants": [
                    {
                        "variant_id_native": "TEST_001_V1",
                        "variant_title": "Test Product 1 - Default",
                        "price_current": "LKR 10,000",
                        "price_original": "LKR 12,000",
                        "currency": "LKR",
                        "availability_text": "In Stock"
                    }
                ],
                "metadata": {
                    "source_website": "test.lk",
                    "shop_contact_phone": "+94111234567",
                    "scrape_timestamp": datetime.utcnow().isoformat()
                }
            },
            {
                "product_id_native": "TEST_002",
                "product_url": "https://test.lk/product/test-product-2",
                "product_title": "Test Product 2 - Another Sample",
                "description_html": "<p>Another test product</p>",
                "brand": "Another Brand",
                "category_path": ["Home", "Test Items"],
                "image_urls": ["https://test.lk/image2.jpg", "https://test.lk/image3.jpg"],
                "variants": [
                    {
                        "variant_id_native": "TEST_002_V1",
                        "variant_title": "Test Product 2 - Variant 1",
                        "price_current": "LKR 5,000",
                        "price_original": None,
                        "currency": "LKR",
                        "availability_text": "Limited Stock"
                    },
                    {
                        "variant_id_native": "TEST_002_V2",
                        "variant_title": "Test Product 2 - Variant 2",
                        "price_current": "LKR 7,500",
                        "price_original": "LKR 8,000",
                        "currency": "LKR",
                        "availability_text": "In Stock"
                    }
                ],
                "metadata": {
                    "source_website": "test.lk",
                    "shop_contact_phone": "+94111234567",
                    "scrape_timestamp": datetime.utcnow().isoformat()
                }
            }
        ]
        
        test_date = datetime.now().strftime('%Y-%m-%d')
        test_source = "test_loading"
        
        # Load the data
        rows_loaded = loader.load_source_data(
            source=test_source,
            products=sample_products,
            scrape_date=test_date,
            file_path="test/sample_data.json"
        )
        
        if rows_loaded == len(sample_products):
            logger.info(f"✅ Successfully loaded {rows_loaded} rows")
        else:
            logger.warning(f"⚠️ Expected {len(sample_products)} rows, loaded {rows_loaded}")
        
        return rows_loaded > 0
        
    except Exception as e:
        logger.error(f"❌ Data loading failed: {str(e)}")
        return False

def test_data_validation():
    """Test data validation after loading"""
    logger.info("🧪 Test 4: Data Validation")
    
    try:
        loader = BigQueryLoader()
        
        test_date = datetime.now().strftime('%Y-%m-%d')
        test_source = "test_loading"
        
        # Validate the data we just loaded
        validation_result = loader.validate_load(test_source, test_date)
        
        logger.info(f"Validation result: {validation_result}")
        
        # Check if validation looks reasonable
        if validation_result.get('row_count', 0) > 0:
            logger.info("✅ Data validation successful")
            return True
        else:
            logger.warning("⚠️ No data found in validation")
            return False
        
    except Exception as e:
        logger.error(f"❌ Data validation failed: {str(e)}")
        return False

def test_sample_retrieval():
    """Test retrieving sample products"""
    logger.info("🧪 Test 5: Sample Product Retrieval")
    
    try:
        loader = BigQueryLoader()
        
        test_date = datetime.now().strftime('%Y-%m-%d')
        test_source = "test_loading"
        
        # Get sample products
        samples = loader.get_sample_products(test_source, test_date, limit=3)
        
        logger.info(f"Retrieved {len(samples)} sample products:")
        for sample in samples:
            logger.info(f"  - {sample.get('title', 'No title')} ({sample.get('brand', 'No brand')})")
        
        if samples:
            logger.info("✅ Sample retrieval successful")
            return True
        else:
            logger.warning("⚠️ No samples retrieved")
            return False
        
    except Exception as e:
        logger.error(f"❌ Sample retrieval failed: {str(e)}")
        return False

def test_multiple_source_loading():
    """Test loading data from multiple sources simultaneously"""
    logger.info("🧪 Test 6: Multiple Source Loading")
    
    try:
        loader = BigQueryLoader()
        
        # Create sample data for multiple sources
        all_source_data = {
            "test_source_1": [
                {
                    "product_id_native": "SRC1_001",
                    "product_title": "Source 1 Product",
                    "brand": "Brand A",
                    "metadata": {"source_website": "source1.lk"}
                }
            ],
            "test_source_2": [
                {
                    "product_id_native": "SRC2_001",
                    "product_title": "Source 2 Product",
                    "brand": "Brand B",
                    "metadata": {"source_website": "source2.lk"}
                },
                {
                    "product_id_native": "SRC2_002",
                    "product_title": "Source 2 Product 2",
                    "brand": "Brand B",
                    "metadata": {"source_website": "source2.lk"}
                }
            ],
            "test_source_3": []  # Empty source to test handling
        }
        
        test_date = datetime.now().strftime('%Y-%m-%d')
        
        # Load all sources
        load_results = loader.load_multiple_sources(all_source_data, test_date)
        
        logger.info(f"Load results: {load_results}")
        
        # Check results
        total_loaded = sum(load_results.values())
        expected_total = sum(len(products) for products in all_source_data.values())
        
        if total_loaded == expected_total:
            logger.info(f"✅ Multiple source loading successful: {total_loaded} rows")
            return True
        else:
            logger.warning(f"⚠️ Expected {expected_total}, loaded {total_loaded}")
            return total_loaded > 0
        
    except Exception as e:
        logger.error(f"❌ Multiple source loading failed: {str(e)}")
        return False

def test_table_stats():
    """Test table statistics retrieval"""
    logger.info("🧪 Test 7: Table Statistics")
    
    try:
        loader = BigQueryLoader()
        
        test_source = "test_loading"
        
        # Get stats for the test table
        stats = loader.get_staging_table_stats(test_source, days=1)
        
        logger.info(f"Table stats: {json.dumps(stats, indent=2, default=str)}")
        
        if 'stats' in stats and stats['stats']:
            logger.info("✅ Table statistics retrieval successful")
            return True
        else:
            logger.warning("⚠️ No statistics found")
            return False
        
    except Exception as e:
        logger.error(f"❌ Table statistics failed: {str(e)}")
        return False

def cleanup_test_data():
    """Clean up test data (optional)"""
    logger.info("🧹 Cleaning up test data...")
    
    try:
        loader = BigQueryLoader()
        
        # List of test tables to clean
        test_tables = [
            "test_appleme", "test_cyberdeals", "test_simplytek",
            "test_loading", "test_source_1", "test_source_2", "test_source_3"
        ]
        
        test_date = datetime.now().strftime('%Y-%m-%d')
        
        for table in test_tables:
            try:
                table_id = f"{loader.project_id}.{loader.staging_dataset}.stg_raw_{table}"
                
                # Delete today's test data only
                query = f"""
                DELETE FROM `{table_id}`
                WHERE scrape_date = '{test_date}'
                AND source_website LIKE 'test%'
                """
                
                job = loader.client.query(query)
                job.result()  # Wait for completion
                
                logger.info(f"🗑️ Cleaned test data from {table}")
                
            except Exception as e:
                logger.debug(f"Could not clean {table}: {str(e)}")
        
        logger.info("✅ Cleanup completed")
        
    except Exception as e:
        logger.warning(f"⚠️ Cleanup failed: {str(e)}")

def run_all_tests():
    """Run all BigQuery loading tests"""
    logger.info("🚀 Starting BigQuery Loading Tests")
    logger.info("=" * 50)
    
    tests = [
        test_dataset_creation,
        test_table_creation,
        test_data_loading,
        test_data_validation,
        test_sample_retrieval,
        test_multiple_source_loading,
        test_table_stats
    ]
    
    results = []
    
    for test_func in tests:
        try:
            result = test_func()
            results.append(result)
            logger.info("")  # Add spacing between tests
        except Exception as e:
            logger.error(f"❌ Test {test_func.__name__} crashed: {str(e)}")
            results.append(False)
    
    # Summary
    passed = sum(results)
    total = len(results)
    
    logger.info("🏁 Test Summary")
    logger.info("=" * 30)
    logger.info(f"Passed: {passed}/{total}")
    logger.info(f"Success Rate: {(passed/total)*100:.1f}%")
    
    if passed == total:
        logger.info("🎉 All tests passed!")
    else:
        logger.warning(f"⚠️ {total - passed} tests failed")
    
    # Offer to clean up
    logger.info("\n🧹 Clean up test data? (y/n)")
    # For automated testing, we'll skip interactive cleanup
    # cleanup_test_data()
    
    return passed == total

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)