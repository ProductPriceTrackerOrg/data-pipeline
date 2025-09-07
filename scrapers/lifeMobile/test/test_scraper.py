"""
Test suite for LifeMobile scraper
"""
import unittest
import json
from datetime import datetime
from unittest.mock import Mock, patch

from models.product_models import Product, ProductVariant, ProductMetadata
from scripts.product_scraper_manager import LifeMobileScrapingManager
from utils.scraper_utils import clean_price, validate_product_data, clean_text


class TestLifeMobileScraper(unittest.TestCase):
    """Test cases for LifeMobile scraper components"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sample_product_data = {
            "product_id_native": "12345",
            "product_url": "https://lifemobile.lk/product/test-phone",
            "product_title": "Test Phone",
            "description_html": "<p>Test description</p>",
            "brand": "Test Brand",
            "category_path": ["Electronics", "Mobile Phones"],
            "specifications": {"RAM": "8GB", "Storage": "128GB"},
            "image_urls": ["https://lifemobile.lk/image1.jpg"],
            "variants": [{
                "variant_id_native": "12345-1",
                "variant_title": "Default",
                "price_current": "50000",
                "price_original": "55000",
                "currency": "Rs.",
                "availability_text": "In stock"
            }],
            "payment_options": [],
            "metadata": {
                "source_website": "lifemobile.lk",
                "shop_contact_phone": "011 2322511",
                "shop_contact_whatsapp": "077 7060616",
                "scrape_timestamp": datetime.now().isoformat()
            }
        }

    def test_product_model_validation(self):
        """Test product model validation"""
        # Valid product should pass
        product = Product(**self.sample_product_data)
        self.assertEqual(product.product_id_native, "12345")
        self.assertEqual(product.product_title, "Test Phone")
        
        # Invalid product should fail
        invalid_data = self.sample_product_data.copy()
        invalid_data['product_title'] = ""  # Empty title
        
        with self.assertRaises(ValueError):
            Product(**invalid_data)

    def test_product_variant_validation(self):
        """Test product variant validation"""
        variant_data = {
            "variant_id_native": "12345-1",
            "variant_title": "Default",
            "price_current": "50000",
            "price_original": "55000",
            "currency": "Rs.",
            "availability_text": "In stock"
        }
        
        variant = ProductVariant(**variant_data)
        self.assertEqual(variant.variant_id_native, "12345-1")
        self.assertEqual(variant.currency, "Rs.")

    def test_clean_price_function(self):
        """Test price cleaning utility"""
        self.assertEqual(clean_price("Rs. 50,000.00"), "50000.00")
        self.assertEqual(clean_price("$1,234.56"), "1234.56")
        self.assertEqual(clean_price(""), "")
        self.assertEqual(clean_price("abc"), "")

    def test_clean_text_function(self):
        """Test text cleaning utility"""
        self.assertEqual(clean_text("  Test   Text  "), "Test Text")
        self.assertEqual(clean_text("Text\nwith\nlines"), "Text with lines")
        self.assertEqual(clean_text(""), "")

    def test_validate_product_data(self):
        """Test product data validation"""
        # Valid data should pass
        self.assertTrue(validate_product_data(self.sample_product_data))
        
        # Missing required field should fail
        invalid_data = self.sample_product_data.copy()
        del invalid_data['product_title']
        self.assertFalse(validate_product_data(invalid_data))
        
        # Empty variants should fail
        invalid_data = self.sample_product_data.copy()
        invalid_data['variants'] = []
        self.assertFalse(validate_product_data(invalid_data))

    def test_scraping_manager_init(self):
        """Test scraping manager initialization"""
        manager = LifeMobileScrapingManager()
        self.assertEqual(manager.output_file, "lifemobile_products.json")
        self.assertTrue(manager.save_locally)
        
        manager2 = LifeMobileScrapingManager("custom.json", False)
        self.assertEqual(manager2.output_file, "custom.json")
        self.assertFalse(manager2.save_locally)

    @patch('scripts.product_scraper_manager.CrawlerProcess')
    def test_run_scraper(self, mock_crawler_process):
        """Test scraper execution"""
        # Mock the crawler process
        mock_process = Mock()
        mock_crawler_process.return_value = mock_process
        
        manager = LifeMobileScrapingManager()
        result = manager.run_scraper()
        
        # Verify crawler was called
        mock_crawler_process.assert_called_once()
        mock_process.crawl.assert_called_once()
        mock_process.start.assert_called_once()

    def test_metadata_datetime_handling(self):
        """Test metadata datetime serialization"""
        metadata = ProductMetadata(
            source_website="lifemobile.lk",
            shop_contact_phone="011 2322511"
        )
        
        # Should have default scrape_timestamp
        self.assertIsInstance(metadata.scrape_timestamp, datetime)

    def test_product_image_url_validation(self):
        """Test image URL validation"""
        data = self.sample_product_data.copy()
        data['image_urls'] = [
            "https://lifemobile.lk/image1.jpg",
            "",  # Empty URL should be filtered out
            "https://lifemobile.lk/image2.jpg",
            "   "  # Whitespace-only should be filtered out
        ]
        
        product = Product(**data)
        # Should only keep valid URLs
        self.assertEqual(len(product.image_urls), 2)
        self.assertIn("https://lifemobile.lk/image1.jpg", product.image_urls)
        self.assertIn("https://lifemobile.lk/image2.jpg", product.image_urls)

    def test_category_path_cleaning(self):
        """Test category path cleaning"""
        data = self.sample_product_data.copy()
        data['category_path'] = ["Electronics", "  ", "Mobile Phones", ""]
        
        product = Product(**data)
        # Empty/whitespace categories should be filtered
        expected_categories = ["Electronics", "Mobile Phones"]
        self.assertEqual(product.category_path, expected_categories)


class TestIntegrationScenarios(unittest.TestCase):
    """Integration test scenarios"""
    
    def test_complete_product_processing(self):
        """Test complete product data processing pipeline"""
        raw_data = {
            "product_id_native": "  12345  ",  # With whitespace
            "product_url": "https://lifemobile.lk/product/test",
            "product_title": "  Test Product  ",  # With whitespace
            "variants": [{
                "variant_id_native": "12345-1",
                "variant_title": "Default",
                "price_current": "Rs. 50,000",  # Needs cleaning
                "availability_text": "In Stock"
            }],
            "metadata": {
                "source_website": "lifemobile.lk",
                "scrape_timestamp": datetime.now().isoformat()
            }
        }
        
        # Should clean and validate properly
        self.assertTrue(validate_product_data(raw_data))
        product = Product(**raw_data)
        
        self.assertEqual(product.product_id_native, "12345")  # Cleaned
        self.assertEqual(product.product_title, "Test Product")  # Cleaned


if __name__ == '__main__':
    unittest.main()