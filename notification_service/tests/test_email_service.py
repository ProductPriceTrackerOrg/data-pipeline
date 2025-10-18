"""
Unit tests for email service module.
"""

import unittest
from unittest.mock import patch, MagicMock

# Import the function to test
from notification_service.email_service import send_notification_email

class TestEmailService(unittest.TestCase):
    """Tests for the email service module."""

    @patch('notification_service.email_service.SendGridAPIClient')
    @patch('notification_service.email_service.os.getenv')
    def test_send_notification_email_success(self, mock_getenv, mock_sendgrid_client):
        """
        Tests successful email sending process.
        """
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            'SENDGRID_API_KEY': 'key here',
            'SENDER_EMAIL': 'pricepulse09@gmail.com'
        }.get(key, default)
        
        # Set up the mock SendGrid client
        mock_sg_instance = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_sg_instance.send.return_value = mock_response
        mock_sendgrid_client.return_value = mock_sg_instance
        
        # Define sample data for the test
        user_email = "anjanapraneeth7@gmail.com"
        product_details = {
            "variant_id": 456,
            "product_title_native": "Super Laptop",
            "old_price": 50000.00,
            "new_price": 45000.00,
            "product_url": "http://example.com/product/456"
        }
        
        # Call the function
        result = send_notification_email(user_email, product_details)
        
        # Assert the results
        self.assertTrue(result)  # Check that the function returned success
        mock_sendgrid_client.assert_called_once_with('key here')
        mock_sg_instance.send.assert_called_once()
        
        # Verify the email content
        sent_message = mock_sg_instance.send.call_args[0][0]
        self.assertEqual(sent_message.from_email.email, "pricepulse09@gmail.com")
        self.assertEqual(sent_message.personalizations[0].tos[0]['email'], user_email)
        self.assertIn("Price Dropped", sent_message.subject)

    @patch('notification_service.email_service.os.getenv')
    def test_send_notification_email_missing_api_key(self, mock_getenv):
        """
        Tests the behavior when the SendGrid API key is missing.
        """
        # Mock missing API key
        mock_getenv.side_effect = lambda key, default=None: {
            'SENDGRID_API_KEY': None,
            'SENDER_EMAIL': 'pricepulse09@gmail.com'
        }.get(key, default)
        
        # Call the function
        result = send_notification_email("user@example.com", {"variant_id": 123})
        
        # Assert the function failed gracefully
        self.assertFalse(result)

    @patch('notification_service.email_service.SendGridAPIClient')
    @patch('notification_service.email_service.os.getenv')
    def test_send_notification_email_price_increase(self, mock_getenv, mock_sendgrid_client):
        """
        Tests that price increases are correctly identified.
        """
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            'SENDGRID_API_KEY': 'key here',
            'SENDER_EMAIL': 'pricepulse09@gmail.com'
        }.get(key, default)
        
        # Set up the mock SendGrid client
        mock_sg_instance = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 202
        mock_sg_instance.send.return_value = mock_response
        mock_sendgrid_client.return_value = mock_sg_instance
        
        # Define product with price increase
        product_details = {
            "variant_id": 789,
            "product_title_native": "Test Product",
            "old_price": 100.00,
            "new_price": 120.00,
            "product_url": "http://example.com/product"
        }
        
        # Call the function
        send_notification_email("anjanapraneeth7@gmail.com", product_details)
        
        # Verify the email subject contains "Increased"
        sent_message = mock_sg_instance.send.call_args[0][0]
        self.assertIn("Increased", sent_message.subject)

    def test_real_email_send(self):
        """
        This test sends a real email using the actual SendGrid API.
        This is an integration test that requires valid API credentials in the .env file.
        """
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv()
        
        # Define sample data for the test
        user_email = "anjanapraneeth7@gmail.com"  # Replace with your email for testing
        product_details = {
            "variant_id": 999,
            "product_title_native": "Test Laptop From Automated Test",
            "old_price": 145000.00,
            "new_price": 125000.00,
            "product_url": "https://example.com/product/999"
        }
        
        # Send an actual email (no mocking)
        import os
        print(f"Using SENDGRID_API_KEY: {os.getenv('SENDGRID_API_KEY')[:5]}...")
        print(f"Using SENDER_EMAIL: {os.getenv('SENDER_EMAIL')}")
        
        # Call the function without any mocking to send a real email
        result = send_notification_email(user_email, product_details)
        
        # Verify success
        self.assertTrue(result, "Email should have been sent successfully")
        print(f"Real email sent to {user_email}")

if __name__ == '__main__':
    unittest.main()