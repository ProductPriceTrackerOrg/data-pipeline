import unittest
from unittest.mock import patch, mock_open
import json
from fact_product_price import main, get_variant_id, get_date_id, is_available

class TestFactProductPrice(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data='{"data": []}')
    @patch("fact_product_price.bigquery.Client")
    def test_empty_json(self, mock_client, mock_file):
        """Test script with an empty JSON file."""
        with patch("fact_product_price.print") as mock_print:
            main()
            mock_print.assert_any_call("Extracted 0 price fact rows.")
            mock_print.assert_any_call("No rows to load.")

    @patch("builtins.open", new_callable=mock_open, read_data='{"data": [{"metadata": {"source_website": "example.com", "scrape_timestamp": "2025-09-08T10:00:00"}, "variants": [{"variant_id_native": "123", "price_current": "100", "price_original": "150", "availability_text": "In Stock"}]}]}')
    @patch("fact_product_price.bigquery.Client")
    def test_valid_json(self, mock_client, mock_file):
        """Test script with a valid JSON file."""
        mock_client.return_value.query.return_value.result.return_value = [type("MockRow", (object,), {"next_id": 1})()]
        with patch("fact_product_price.print") as mock_print:
            main()
            mock_print.assert_any_call("Extracted 1 price fact rows.")

    def test_get_variant_id(self):
        """Test variant ID generation."""
        variant_id = get_variant_id("example.com", "123")
        self.assertIsInstance(variant_id, int)

    def test_get_date_id(self):
        """Test date ID generation."""
        date_id = get_date_id("2025-09-08T10:00:00")
        self.assertEqual(date_id, 20250908)

    def test_is_available(self):
        """Test availability check."""
        self.assertTrue(is_available("In Stock"))
        self.assertFalse(is_available("Out of Stock"))

if __name__ == "__main__":
    unittest.main()
