"""
Unit tests for PostgreSQL queries module.
"""

import unittest
from unittest.mock import MagicMock

# Import the function to test
from notification_service.postgres_queries import get_subscribed_users

class TestPostgresQueries(unittest.TestCase):
    """Tests for the PostgreSQL queries module."""

    def test_get_subscribed_users(self):
        """
        Tests that the get_subscribed_users function executes the correct query
        and returns a list of emails.
        """
        # 1. Setup the mock
        # Create a mock database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Define the fake data the database should return
        mock_cursor.fetchall.return_value = [("user1@example.com",), ("user2@example.com",)]

        # 2. Call the function
        variant_id_to_test = 123
        subscribed_users = get_subscribed_users(variant_id_to_test, mock_conn)
        
        # 3. Assert the results
        # Check that the execute method was called
        mock_cursor.execute.assert_called_once()
        # Check that the correct variant_id was passed as a parameter
        self.assertEqual(mock_cursor.execute.call_args[0][1], (variant_id_to_test,))
        # Check that the function returned the correct emails
        self.assertEqual(len(subscribed_users), 2)
        self.assertIn("user1@example.com", subscribed_users)
        self.assertIn("user2@example.com", subscribed_users)
    
    def test_get_subscribed_users_empty(self):
        """Tests when no subscribed users are found."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Define empty results
        mock_cursor.fetchall.return_value = []
        
        # Call the function
        subscribed_users = get_subscribed_users(456, mock_conn)
        
        # Check results
        self.assertEqual(len(subscribed_users), 0)

    def test_get_subscribed_users_exception(self):
        """Tests error handling when database query fails."""
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("Database error")
        
        # Call the function
        subscribed_users = get_subscribed_users(789, mock_conn)
        
        # Check that the function gracefully returns an empty list on error
        self.assertEqual(len(subscribed_users), 0)

    def test_real_supabase_connection(self):
        """
        Tests the PostgreSQL query with a real Supabase connection.
        This test requires actual Supabase credentials in the .env file.
        """
        # Import required modules
        from dotenv import load_dotenv
        import os
        import psycopg2
        
        # Load environment variables
        load_dotenv()
        
        # Test with actual product ID
        test_variant_id = 439031072
        
        # Create an actual database connection
        conn = None
        try:
            # Extract connection details from the Supabase URL
            supabase_url = os.getenv("SUPABASE_URL", "")
            if supabase_url and "supabase.co" in supabase_url:
                # Extract host from the URL
                host = supabase_url.replace("https://", "").replace("http://", "")
                print(f"Connecting to Supabase host: {host}")
                
                # Connect using Supabase credentials
                conn = psycopg2.connect(
                    dbname="postgres", 
                    user="postgres",
                    password=os.getenv("SUPABASE_JWT_SECRET", ""),
                    host=host,
                    port="5432"
                )
                
                # Get actual subscribed users
                subscribed_users = get_subscribed_users(test_variant_id, conn)
                
                # Print results
                print(f"Found {len(subscribed_users)} users subscribed to product {test_variant_id}")
                if subscribed_users:
                    print(f"First few emails: {subscribed_users[:3]}")
                
                # Test should pass even if no users found, as long as connection works
                self.assertIsNotNone(subscribed_users)
                
            else:
                self.skipTest("Supabase URL not configured in .env file")
        except Exception as e:
            self.fail(f"Failed to connect to Supabase: {str(e)}")
        finally:
            if conn:
                conn.close()

if __name__ == '__main__':
    unittest.main()