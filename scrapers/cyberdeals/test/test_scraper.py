"""
Test cases for the CyberDeals scraper
"""
import unittest
import asyncio
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.product_scraper_core import AsyncCyberDealsScraper
from utils.scraper_utils import detect_brand, extract_image_urls
from config.scraper_config import KNOWN_BRANDS

class TestCyberDealsScraper(unittest.TestCase):
    def setUp(self):
        self.scraper = AsyncCyberDealsScraper()
        
    def test_brand_detection(self):
        """Test brand detection logic"""
        # Test with brand in categories
        categories = ["Apple Products", "iPhone", "Smartphones"]
        self.assertEqual(
            detect_brand("iPhone 13", categories, "https://cyberdeals.lk/iphone-13", KNOWN_BRANDS), 
            "Apple"
        )
        
        # Test with brand in title
        self.assertEqual(
            detect_brand("Samsung Galaxy S21", [], "https://cyberdeals.lk/galaxy-s21", KNOWN_BRANDS), 
            "Samsung"
        )
        
        # Test with brand in URL
        self.assertEqual(
            detect_brand("Galaxy S21", [], "https://cyberdeals.lk/samsung-galaxy-s21", KNOWN_BRANDS), 
            "Samsung"
        )
        
        # Test special case for TP-Link
        self.assertEqual(
            detect_brand("TP-Link Router", [], "https://cyberdeals.lk/tp-link-router", KNOWN_BRANDS), 
            "TP-Link"
        )
        
    @patch('httpx.AsyncClient')
    def test_fetch_page(self, mock_client):
        """Test page fetching with retry logic"""
        mock_response = MagicMock()
        mock_response.text = "<html><body>Test</body></html>"
        mock_client.get.return_value.__aenter__.return_value = mock_response
        
        async def run_test():
            result = await self.scraper.fetch_page(mock_client, "https://cyberdeals.lk/test")
            self.assertEqual(result, "<html><body>Test</body></html>")
            
        asyncio.run(run_test())
        
if __name__ == '__main__':
    unittest.main()
