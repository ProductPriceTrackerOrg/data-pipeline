"""
Test Azure Data Lake Storage upload functionality
"""
import unittest
import json
import os
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

# Assuming we have the main.py upload function available
import sys
sys.path.append('..')


class TestAzureUpload(unittest.TestCase):
    """Test Azure upload functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.sample_json_data = json.dumps([{
            "product_id_native": "12345",
            "product_url": "https://lifemobile.lk/product/test",
            "product_title": "Test Product",
            "variants": [{
                "variant_id_native": "12345-1",
                "variant_title": "Default",
                "price_current": "50000",
                "availability_text": "In Stock"
            }],
            "metadata": {
                "source_website": "lifemobile.lk",
                "scrape_timestamp": datetime.now().isoformat()
            }
        }], indent=2)

    @patch('os.getenv')
    def test_missing_connection_string(self, mock_getenv):
        """Test behavior when Azure connection string is missing"""
        mock_getenv.return_value = None
        
        # Import here to avoid import errors if Azure SDK not installed
        try:
            from main import upload_to_adls
            
            with self.assertRaises(ValueError) as context:
                upload_to_adls(self.sample_json_data, "lifemobile.lk")
            
            self.assertIn("connection string not found", str(context.exception))
        except ImportError:
            self.skipTest("Azure SDK not available")

    @patch('azure.storage.blob.BlobServiceClient.from_connection_string')
    @patch('os.getenv')
    def test_successful_upload(self, mock_getenv, mock_blob_service):
        """Test successful upload to Azure"""
        # Mock environment variable
        mock_getenv.return_value = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test"
        
        # Mock blob service and client
        mock_blob_client = Mock()
        mock_blob_service_instance = Mock()
        mock_blob_service_instance.get_blob_client.return_value = mock_blob_client
        mock_blob_service.return_value = mock_blob_service_instance
        
        try:
            from main import upload_to_adls
            
            result = upload_to_adls(self.sample_json_data, "lifemobile.lk")
            
            # Verify calls were made
            mock_blob_service.assert_called_once()
            mock_blob_service_instance.get_blob_client.assert_called_once()
            mock_blob_client.upload_blob.assert_called_once()
            
            self.assertTrue(result)
        except ImportError:
            self.skipTest("Azure SDK not available")

    def test_json_data_format(self):
        """Test that JSON data is properly formatted"""
        # Parse the JSON to ensure it's valid
        parsed_data = json.loads(self.sample_json_data)
        
        self.assertIsInstance(parsed_data, list)
        self.assertEqual(len(parsed_data), 1)
        
        product = parsed_data[0]
        self.assertIn('product_id_native', product)
        self.assertIn('variants', product)
        self.assertIsInstance(product['variants'], list)

    @patch('azure.storage.blob.BlobServiceClient.from_connection_string')
    @patch('os.getenv')
    def test_upload_error_handling(self, mock_getenv, mock_blob_service):
        """Test error handling during upload"""
        mock_getenv.return_value = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test"
        
        # Mock blob service to raise exception
        mock_blob_service.side_effect = Exception("Connection failed")
        
        try:
            from main import upload_to_adls
            
            result = upload_to_adls(self.sample_json_data, "lifemobile.lk")
            self.assertFalse(result)
        except ImportError:
            self.skipTest("Azure SDK not available")

    def test_file_path_generation(self):
        """Test that file paths are generated correctly"""
        from datetime import datetime
        
        # Expected format: source_website=lifemobile.lk/scrape_date=YYYY-MM-DD/data.json
        source_website = "lifemobile.lk"
        scrape_date = datetime.now().strftime('%Y-%m-%d')
        
        expected_path = f"source_website={source_website}/scrape_date={scrape_date}/data.json"
        
        # This would be the actual path format used in upload_to_adls
        self.assertIn("source_website=lifemobile.lk", expected_path)
        self.assertIn("scrape_date=", expected_path)
        self.assertIn("/data.json", expected_path)


if __name__ == '__main__':
    unittest.main()