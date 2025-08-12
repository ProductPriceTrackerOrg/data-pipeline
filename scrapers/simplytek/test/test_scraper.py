#!/usr/bin/env python3
"""
Comprehensive test suite for SimplyTek scraper
Tests data validation, scraping functionality, and performance
"""

import asyncio
import sys
import unittest
import json
import os
import tempfile
import logging
from typing import Dict, Any, List
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock

# Import our modules
from scrapers.simplytek.models.product_models import (
    Product, ProductVariant, ProductMetadata, ScrapingResult,
    ShopifyProduct, ShopifyApiResponse
)
from scrapers.simplytek.scripts.product_scraper_core import ProductScraper
from scrapers.simplytek.scripts.product_scraper_manager import ScrapingManager
from scrapers.simplytek.utils.scraper_utils import ScrapingSession, format_price, get_availability_text
from scrapers.simplytek.config.scraper_config import SHOP_METADATA, DEFAULT_CURRENCY


class TestDataModels(unittest.TestCase):
    """Test Pydantic data models"""
    
    def setUp(self):
        """Set up test data"""
        self.sample_variant = {
            "variant_id_native": "12345",
            "variant_title": "Black 64GB",
            "price_current": "25000.00",
            "price_original": "30000.00",
            "currency": "LKR",
            "availability_text": "In Stock"
        }
        
        self.sample_metadata = {
            "source_website": "www.simplytek.lk",
            "shop_contact_phone": "+94 117 555 888",
            "shop_contact_whatsapp": "+94 72 672 9729"
        }
        
        self.sample_product = {
            "product_id_native": "9046548742463",
            "product_url": "https://www.simplytek.lk/products/amazfit-active",
            "product_title": "Amazfit Active Smartwatch",
            "description_html": "<p>Great smartwatch</p>",
            "brand": "Amazfit",
            "category_path": ["Electronics", "Smartwatches"],
            "image_urls": ["https://example.com/image1.jpg"],
            "variants": [self.sample_variant],
            "metadata": self.sample_metadata
        }
    
    def test_product_variant_validation(self):
        """Test ProductVariant model validation"""
        # Valid variant
        variant = ProductVariant(**self.sample_variant)
        self.assertEqual(variant.variant_id_native, "12345")
        self.assertEqual(variant.price_current, "25000.00")
        
        # Invalid variant (missing required field)
        with self.assertRaises(ValueError):
            ProductVariant(variant_title="Test", price_current="")
    
    def test_product_validation(self):
        """Test Product model validation"""
        # Valid product
        product = Product(**self.sample_product)
        self.assertEqual(product.product_id_native, "9046548742463")
        self.assertEqual(len(product.variants), 1)
        
        # Invalid product (no variants)
        invalid_product = self.sample_product.copy()
        invalid_product["variants"] = []
        
        with self.assertRaises(ValueError):
            Product(**invalid_product)
    
    def test_scraping_result_validation(self):
        """Test ScrapingResult model validation"""
        result_data = {
            "total_products": 1,
            "total_variants": 1,
            "categories_scraped": ["smartwatches"],
            "scraping_metadata": {"test": "data"},
            "products": [self.sample_product]
        }
        
        result = ScrapingResult(**result_data)
        self.assertEqual(result.total_products, 1)
        self.assertEqual(len(result.products), 1)


class TestScrapingUtils(unittest.TestCase):
    """Test scraping utilities"""
    
    def test_format_price(self):
        """Test price formatting"""
        self.assertEqual(format_price("25000"), "25000.00")
        self.assertEqual(format_price("25,000.50"), "25000.50")
        self.assertEqual(format_price(""), "0.00")
        self.assertEqual(format_price("invalid"), "invalid")
    
    def test_availability_text(self):
        """Test availability text conversion"""
        self.assertEqual(get_availability_text(True), "In Stock")
        self.assertEqual(get_availability_text(False), "Out of Stock")
    
    @patch('scraper_utils.os.makedirs')
    def test_ensure_directory(self, mock_makedirs):
        """Test directory creation"""
        from scrapers.simplytek.utils.scraper_utils import ensure_output_directory
        
        ensure_output_directory("test_dir")
        mock_makedirs.assert_called_once_with("test_dir")


class TestProductScraper(unittest.TestCase):
    """Test product scraper functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.scraper = ProductScraper()
        
        # Sample Shopify API response
        self.sample_shopify_product = {
            "id": 9046548742463,
            "title": "Amazfit Active Smartwatch",
            "handle": "amazfit-active-smartwatch",
            "body_html": "<p>Great smartwatch</p>",
            "vendor": "Amazfit",
            "variants": [
                {
                    "id": 47487777571135,
                    "title": "Black",
                    "price": "27999.00",
                    "compare_at_price": "39999.00",
                    "available": True,
                    "product_id": 9046548742463
                }
            ],
            "images": [
                {
                    "id": 43674011140415,
                    "src": "https://example.com/image.jpg",
                    "variant_ids": [47487777571135]
                }
            ]
        }
    
    async def test_transform_shopify_product(self):
        """Test Shopify product transformation"""
        product = await self.scraper._transform_shopify_product(
            self.sample_shopify_product, 
            "smartwatches"
        )
        
        self.assertIsNotNone(product)
        self.assertEqual(product.product_id_native, "9046548742463")
        self.assertEqual(product.product_title, "Amazfit Active Smartwatch")
        self.assertEqual(len(product.variants), 1)
        self.assertEqual(product.variants[0].price_current, "27999.00")
    
    def test_build_category_path(self):
        """Test category path building"""
        category_path = self.scraper._build_category_path("smartwatches")
        self.assertIsInstance(category_path, list)
        self.assertTrue(len(category_path) > 0)
    
    def test_extract_image_urls(self):
        """Test image URL extraction"""
        images = [
            {"src": "https://example.com/image1.jpg"},
            {"src": "https://example.com/image2.jpg"},
            {"src": ""},  # Empty URL should be filtered out
            {"src": "invalid-url"}  # Invalid URL should be filtered out
        ]
        
        urls = self.scraper._extract_image_urls(images)
        self.assertEqual(len(urls), 2)  # Only 2 valid URLs


class TestScrapingManager(unittest.TestCase):
    """Test scraping manager"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.manager = ScrapingManager(
            output_dir=self.temp_dir,
            output_file="test_products.json"
        )
    
    def tearDown(self):
        """Clean up test environment"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    @patch('product_scraper_manager.ProductScraper')
    async def test_run_full_scraping(self, mock_scraper_class):
        """Test full scraping process"""
        # Mock the scraper
        mock_scraper = AsyncMock()
        mock_result = ScrapingResult(
            total_products=10,
            total_variants=15,
            categories_scraped=["test"],
            scraping_metadata={},
            products=[]
        )
        mock_scraper.scrape_all_products.return_value = mock_result
        mock_scraper_class.return_value = mock_scraper
        
        # Run scraping
        result = await self.manager.run_full_scraping()
        
        self.assertEqual(result.total_products, 10)
        self.assertEqual(result.total_variants, 15)
        
        # Check if file was created
        self.assertTrue(os.path.exists(self.manager.output_path))
    
    async def test_validate_scraped_data(self):
        """Test data validation"""
        # Create sample data file
        sample_data = {
            "total_products": 1,
            "total_variants": 1,
            "categories_scraped": ["test"],
            "scraping_metadata": {},
            "products": [
                {
                    "product_id_native": "123",
                    "product_url": "https://example.com/product",
                    "product_title": "Test Product",
                    "description_html": None,
                    "brand": None,
                    "category_path": ["Electronics"],
                    "image_urls": [],
                    "variants": [
                        {
                            "variant_id_native": "456",
                            "variant_title": "Default",
                            "price_current": "100.00",
                            "price_original": None,
                            "currency": "LKR",
                            "availability_text": "In Stock"
                        }
                    ],
                    "metadata": {
                        "source_website": "test.com",
                        "shop_contact_phone": None,
                        "shop_contact_whatsapp": None,
                        "scrape_timestamp": datetime.now().isoformat()
                    }
                }
            ]
        }
        
        # Save sample data
        with open(self.manager.output_path, 'w') as f:
            json.dump(sample_data, f, default=str)
        
        # Validate
        report = await self.manager.validate_scraped_data()
        
        self.assertTrue(report["valid"])
        self.assertEqual(report["total_products"], 1)


class TestIntegration(unittest.TestCase):
    """Integration tests"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Disable actual HTTP requests for tests
        self.mock_session_patcher = patch('product_scraper_core.ScrapingSession')
        self.mock_session = self.mock_session_patcher.start()
    
    def tearDown(self):
        """Clean up integration test environment"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        self.mock_session_patcher.stop()
    
    async def test_end_to_end_scraping(self):
        """Test complete scraping workflow"""
        # Mock HTTP responses
        mock_session_instance = AsyncMock()
        mock_response_data = {
            "products": [
                {
                    "id": 123456,
                    "title": "Test Product",
                    "handle": "test-product",
                    "body_html": "<p>Test description</p>",
                    "vendor": "TestBrand",
                    "variants": [
                        {
                            "id": 789012,
                            "title": "Default Title",
                            "price": "1000.00",
                            "compare_at_price": None,
                            "available": True,
                            "product_id": 123456
                        }
                    ],
                    "images": [
                        {"src": "https://example.com/test.jpg"}
                    ]
                }
            ]
        }
        
        mock_session_instance.fetch_multiple_pages.return_value = [mock_response_data]
        mock_session_instance.get_stats.return_value = {
            "total_requests": 1,
            "successful_requests": 1,
            "failed_requests": 0,
            "success_rate": 100.0
        }
        
        self.mock_session.return_value.__aenter__.return_value = mock_session_instance
        
        # Run scraping
        manager = ScrapingManager(
            output_dir=self.temp_dir,
            output_file="integration_test.json"
        )
        
        result = await manager.run_full_scraping()
        
        # Verify results
        self.assertGreater(result.total_products, 0)
        self.assertTrue(os.path.exists(manager.output_path))
        
        # Load and verify saved data
        with open(manager.output_path, 'r') as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data["total_products"], result.total_products)


class TestPerformance(unittest.TestCase):
    """Performance tests"""
    
    @patch('product_scraper_core.ScrapingSession')
    async def test_concurrent_scraping_performance(self, mock_session):
        """Test concurrent scraping performance"""
        # Mock concurrent responses
        mock_session_instance = AsyncMock()
        
        # Simulate multiple product responses
        products = []
        for i in range(100):  # Simulate 100 products
            products.append({
                "id": 1000 + i,
                "title": f"Product {i}",
                "handle": f"product-{i}",
                "body_html": f"<p>Description {i}</p>",
                "vendor": "TestBrand",
                "variants": [
                    {
                        "id": 2000 + i,
                        "title": "Default Title",
                        "price": f"{1000 + i}.00",
                        "compare_at_price": None,
                        "available": True,
                        "product_id": 1000 + i
                    }
                ],
                "images": []
            })
        
        mock_response = {"products": products}
        mock_session_instance.fetch_multiple_pages.return_value = [mock_response]
        mock_session_instance.get_stats.return_value = {
            "total_requests": 1,
            "successful_requests": 1,
            "failed_requests": 0,
            "success_rate": 100.0
        }
        
        mock_session.return_value.__aenter__.return_value = mock_session_instance
        
        # Measure performance
        start_time = datetime.now()
        
        scraper = ProductScraper()
        result = await scraper.scrape_all_products()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Performance assertions
        self.assertEqual(result.total_products, 100)
        self.assertLess(duration, 10)  # Should complete within 10 seconds
        
        print(f"Performance test: Processed {result.total_products} products in {duration:.2f} seconds")


def create_test_data_file(filepath: str) -> None:
    """Create a test data file for validation testing"""
    test_data = {
        "total_products": 2,
        "total_variants": 3,
        "categories_scraped": ["mobile-phones", "smartwatches"],
        "scraping_metadata": {
            "scraping_duration": "0:05:30",
            "session_stats": {
                "total_requests": 10,
                "successful_requests": 10,
                "failed_requests": 0,
                "success_rate": 100.0
            }
        },
        "products": [
            {
                "product_id_native": "9046548742463",
                "product_url": "https://www.simplytek.lk/products/amazfit-active",
                "product_title": "Amazfit Active Calling Smartwatch",
                "description_html": "<h5>An Active Body Needs a Recovered Mind...</h5>",
                "brand": "Amazfit",
                "category_path": ["Electronics", "Wearables", "Smartwatches"],
                "image_urls": [
                    "https://cdn.shopify.com/s/files/1/0822/2058/1183/files/AMAZFIT-active-black_2.jpg"
                ],
                "variants": [
                    {
                        "variant_id_native": "47487777571135",
                        "variant_title": "Black",
                        "price_current": "27999.00",
                        "price_original": "39999.00",
                        "currency": "LKR",
                        "availability_text": "Out of Stock"
                    },
                    {
                        "variant_id_native": "47487777603903",
                        "variant_title": "Pink",
                        "price_current": "27999.00",
                        "price_original": "39999.00",
                        "currency": "LKR",
                        "availability_text": "Out of Stock"
                    }
                ],
                "metadata": {
                    "source_website": "www.simplytek.lk",
                    "shop_contact_phone": "+94 117 555 888",
                    "shop_contact_whatsapp": "+94 72 672 9729",
                    "scrape_timestamp": datetime.now().isoformat()
                }
            },
            {
                "product_id_native": "1234567890123",
                "product_url": "https://www.simplytek.lk/products/sample-phone",
                "product_title": "Sample Mobile Phone",
                "description_html": "<p>A great mobile phone</p>",
                "brand": "SampleBrand",
                "category_path": ["Electronics", "Mobile Phones"],
                "image_urls": [
                    "https://example.com/phone1.jpg",
                    "https://example.com/phone2.jpg"
                ],
                "variants": [
                    {
                        "variant_id_native": "9876543210987",
                        "variant_title": "128GB Blue",
                        "price_current": "45000.00",
                        "price_original": None,
                        "currency": "LKR",
                        "availability_text": "In Stock"
                    }
                ],
                "metadata": {
                    "source_website": "www.simplytek.lk",
                    "shop_contact_phone": "+94 117 555 888",
                    "shop_contact_whatsapp": "+94 72 672 9729",
                    "scrape_timestamp": datetime.now().isoformat()
                }
            }
        ]
    }
    
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(test_data, f, indent=2, ensure_ascii=False)


async def run_performance_benchmark():
    """Run performance benchmark"""
    print("Running performance benchmark...")
    
    # Create temporary test environment
    temp_dir = tempfile.mkdtemp()
    
    try:
        with patch('product_scraper_core.ScrapingSession') as mock_session:
            # Mock high-volume response
            products = []
            for i in range(1000):
                products.append({
                    "id": i,
                    "title": f"Benchmark Product {i}",
                    "handle": f"benchmark-product-{i}",
                    "body_html": f"<p>Benchmark description {i}</p>",
                    "vendor": "BenchmarkBrand",
                    "variants": [
                        {
                            "id": i * 10,
                            "title": f"Variant {i}",
                            "price": f"{1000 + i}.00",
                            "available": i % 2 == 0,
                            "product_id": i
                        }
                    ],
                    "images": [
                        {"src": f"https://example.com/image{i}.jpg"}
                    ]
                })
            
            mock_session_instance = AsyncMock()
            mock_session_instance.fetch_multiple_pages.return_value = [{"products": products}]
            mock_session_instance.get_stats.return_value = {
                "total_requests": 1,
                "successful_requests": 1,
                "failed_requests": 0,
                "success_rate": 100.0
            }
            mock_session.return_value.__aenter__.return_value = mock_session_instance
            
            # Run benchmark
            start_time = datetime.now()
            
            manager = ScrapingManager(output_dir=temp_dir, output_file="benchmark.json")
            result = await manager.run_full_scraping()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Calculate metrics
            products_per_second = result.total_products / duration if duration > 0 else 0
            variants_per_second = result.total_variants / duration if duration > 0 else 0
            
            print(f"\n{'='*60}")
            print("PERFORMANCE BENCHMARK RESULTS")
            print(f"{'='*60}")
            print(f"Products processed: {result.total_products:,}")
            print(f"Variants processed: {result.total_variants:,}")
            print(f"Duration: {duration:.2f} seconds")
            print(f"Products/second: {products_per_second:.2f}")
            print(f"Variants/second: {variants_per_second:.2f}")
            print(f"{'='*60}")
    
    finally:
        # Clean up
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    """Main test runner"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'benchmark':
        # Run performance benchmark
        asyncio.run(run_performance_benchmark())
        return
    
    if len(sys.argv) > 1 and sys.argv[1] == 'create-test-data':
        # Create test data file
        output_dir = sys.argv[2] if len(sys.argv) > 2 else "test_data"
        filepath = os.path.join(output_dir, "test_simplytek_products.json")
        create_test_data_file(filepath)
        print(f"Test data file created: {filepath}")
        return
    
    # Setup logging for tests
    logging.basicConfig(level=logging.WARNING)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_suite.addTest(unittest.makeSuite(TestDataModels))
    test_suite.addTest(unittest.makeSuite(TestScrapingUtils))
    test_suite.addTest(unittest.makeSuite(TestProductScraper))
    test_suite.addTest(unittest.makeSuite(TestScrapingManager))
    test_suite.addTest(unittest.makeSuite(TestIntegration))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Return appropriate exit code
    if result.wasSuccessful():
        print("\n✅ All tests passed!")
        return 0
    else:
        print(f"\n❌ {len(result.failures)} test(s) failed, {len(result.errors)} error(s)")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)