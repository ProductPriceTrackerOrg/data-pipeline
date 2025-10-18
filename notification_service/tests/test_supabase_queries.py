"""
Unit tests for Supabase queries module.
"""

import unittest
from unittest.mock import patch, MagicMock

# Import the function to test
from notification_service.supabase_queries import get_subscribed_users_supabase, get_subscribed_users

class TestSupabaseQueries(unittest.TestCase):
    """Tests for the Supabase queries module."""

    @patch('notification_service.supabase_queries.create_client')
    @patch('notification_service.supabase_queries.os.getenv')
    def test_get_subscribed_users_supabase(self, mock_getenv, mock_create_client):
        """
        Tests the get_subscribed_users_supabase function.
        """
        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            'SUPABASE_URL': 'https://oumycjiawnuppwsboabf.supabase.co',
            'SUPABASE_KEY': 'test_key'
        }.get(key, default)
        
        # Set up mock Supabase client
        mock_client = MagicMock()
        mock_create_client.return_value = mock_client
        
        # Set up mock responses for table queries
        mock_favorites_response = MagicMock()
        mock_favorites_response.data = [
            {'user_id': 'user1'},
            {'user_id': 'user2'}
        ]
        
        mock_profile_response = MagicMock()
        mock_profile_response.data = [
            {'email': 'user1@example.com', 'is_active': True}
        ]
        
        mock_settings_response = MagicMock()
        mock_settings_response.data = [
            {'notify_on_price_drop': True}
        ]
        
        # Configure the mock client to return our mock responses
        mock_client.table.return_value.select.return_value.eq.return_value.execute.return_value = mock_favorites_response
        mock_client.from_.return_value.select.return_value.eq.return_value.execute.side_effect = [
            mock_profile_response, mock_settings_response
        ]
        
        # Call the function
        variant_id = 12345
        result = get_subscribed_users_supabase(variant_id)
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], 'user1@example.com')
        
        # Verify that the client was called correctly
        mock_create_client.assert_called_once_with(
            'https://oumycjiawnuppwsboabf.supabase.co', 'test_key'
        )
        
    def test_get_subscribed_users_delegation(self):
        """Test that get_subscribed_users delegates to get_subscribed_users_supabase."""
        with patch('notification_service.supabase_queries.get_subscribed_users_supabase') as mock_supabase_func:
            mock_supabase_func.return_value = ['test@example.com']
            
            # Call the delegating function
            result = get_subscribed_users(123)
            
            # Verify it calls the Supabase function
            mock_supabase_func.assert_called_once_with(123)
            self.assertEqual(result, ['test@example.com'])

if __name__ == '__main__':
    unittest.main()