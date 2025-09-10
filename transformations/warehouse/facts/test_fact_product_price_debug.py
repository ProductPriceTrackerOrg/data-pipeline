"""
Comprehensive test to debug fact_product_price.py extraction issues
"""
import unittest
import json
import os
import tempfile
from unittest.mock import patch, MagicMock
from fact_product_price import main, get_variant_id, get_date_id, is_available

class TestFactProductPriceDebug(unittest.TestCase):
    
    def test_file_exists(self):
        """Test if the hardcoded JSON file exists"""
        json_path = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "extraction", "temp_data", "extracted_data_2025-09-03_100700.json"
        )
        print(f"Looking for file: {json_path}")
        print(f"File exists: {os.path.exists(json_path)}")
        
        # List files in the directory to see what's actually there
        temp_data_dir = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "extraction", "temp_data"
        )
        if os.path.exists(temp_data_dir):
            files = os.listdir(temp_data_dir)
            print(f"Files in temp_data directory: {files}")
        else:
            print("temp_data directory does not exist!")
    
    def test_sample_json_structure(self):
        """Test with your actual sample JSON structure"""
        sample_data = [
            {
                "product_id_native": "37142",
                "product_url": "https://appleme.lk/product/knee-massager-wireless-k1/",
                "product_title": "Knee Massager Wireless K1",
                "variants": [
                    {
                        "variant_id_native": "37142",
                        "variant_title": "Knee Massager Wireless K1",
                        "price_current": "16817.02",
                        "price_original": None,
                        "currency": "LKR",
                        "availability_text": "✓ In Stock"
                    }
                ],
                "metadata": {
                    "source_website": "appleme.lk",
                    "scrape_timestamp": "2025-09-03T04:55:14.966430"
                }
            }
        ]
        
        # Create temporary file with this structure
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_data, f)
            temp_file = f.name
        
        try:
            # Patch the file path to use our temporary file
            with patch('fact_product_price.EXTRACTED_JSON_PATH', temp_file):
                with patch('fact_product_price.bigquery.Client') as mock_client:
                    # Mock BigQuery client
                    mock_result = MagicMock()
                    mock_result.next_id = 0
                    mock_client.return_value.query.return_value.result.return_value = [mock_result]
                    
                    # Mock the load_table_from_json method
                    mock_job = MagicMock()
                    mock_client.return_value.load_table_from_json.return_value = mock_job
                    
                    # Capture print statements
                    with patch('builtins.print') as mock_print:
                        main()
                        
                        # Check what was printed
                        print_calls = [call.args[0] for call in mock_print.call_args_list]
                        print("Print statements during execution:")
                        for statement in print_calls:
                            print(f"  {statement}")
                        
                        # Verify expected behavior
                        self.assertTrue(any("Sample product structure:" in str(call) for call in print_calls))
                        self.assertTrue(any("Extracted" in str(call) for call in print_calls))
        finally:
            os.unlink(temp_file)
    
    def test_utility_functions(self):
        """Test individual utility functions"""
        # Test get_variant_id
        variant_id = get_variant_id("appleme.lk", "37142")
        print(f"Generated variant_id: {variant_id}")
        self.assertIsInstance(variant_id, int)
        
        # Test get_date_id
        date_id = get_date_id("2025-09-03T04:55:14.966430")
        print(f"Generated date_id: {date_id}")
        self.assertEqual(date_id, 20250903)
        
        # Test is_available
        self.assertTrue(is_available("✓ In Stock"))
        self.assertFalse(is_available("Sold Out"))
        self.assertTrue(is_available("In Stock"))
    
    def test_empty_file(self):
        """Test with empty JSON file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([], f)
            temp_file = f.name
        
        try:
            with patch('fact_product_price.EXTRACTED_JSON_PATH', temp_file):
                with patch('builtins.print') as mock_print:
                    main()
                    print_calls = [call.args[0] for call in mock_print.call_args_list]
                    self.assertTrue(any("Extracted 0 price fact rows." in str(call) for call in print_calls))
        finally:
            os.unlink(temp_file)
    
    def test_malformed_json(self):
        """Test with malformed JSON"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not valid json")
            temp_file = f.name
        
        try:
            with patch('fact_product_price.EXTRACTED_JSON_PATH', temp_file):
                with patch('builtins.print') as mock_print:
                    main()
                    print_calls = [call.args[0] for call in mock_print.call_args_list]
                    self.assertTrue(any("Failed to load JSON file" in str(call) for call in print_calls))
        finally:
            os.unlink(temp_file)

if __name__ == '__main__':
    unittest.main(verbosity=2)
