"""
Unit tests for BigQuery queries module.
"""

import unittest
from unittest.mock import patch, MagicMock

# Import the function to test
from notification_service.bigquery_queries import get_price_changes

class TestBigQueryQueries(unittest.TestCase):
    """Tests for the BigQuery queries module."""

    @patch('notification_service.bigquery_queries.bigquery.Client')
    def test_get_price_changes(self, mock_bigquery_client):
        """
        Tests that the get_price_changes function correctly calls the BigQuery client
        and returns the processed results.
        """
        # 1. Setup the mock
        # Define the fake data that the BigQuery client should return
        mock_rows = [
            {
                "variant_id": 101, 
                "shop_product_id": 201,
                "product_title_native": "Test Product A", 
                "product_url": "http://example.com/product-a",
                "current_price": 90.0, 
                "previous_price": 100.0
            },
            {
                "variant_id": 102, 
                "shop_product_id": 202,
                "product_title_native": "Test Product B", 
                "product_url": "http://example.com/product-b",
                "current_price": 110.0, 
                "previous_price": 105.0
            }
        ]
        
        # Configure the mock client instance
        mock_instance = MagicMock()
        mock_result = MagicMock()
        mock_result.result.return_value = mock_rows
        mock_instance.query.return_value = mock_result
        mock_bigquery_client.return_value = mock_instance
        
        # 2. Call the function
        price_changes = get_price_changes()
        
        # 3. Assert the results
        # Check that the BigQuery client was initialized
        mock_bigquery_client.assert_called_once()
        # Check that the query method was called
        mock_instance.query.assert_called_once()
        # Check that the function returned the data we told it to
        self.assertEqual(len(price_changes), 2)
        self.assertEqual(price_changes[0]['variant_id'], 101)
        self.assertEqual(price_changes[0]['product_title_native'], "Test Product A")
        self.assertEqual(price_changes[1]['variant_id'], 102)

if __name__ == '__main__':
    unittest.main()