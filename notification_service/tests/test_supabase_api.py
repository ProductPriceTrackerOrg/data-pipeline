"""
Integration tests for Supabase REST API.
"""

import unittest
import os
from dotenv import load_dotenv
from supabase import create_client, Client


class TestSupabaseAPI(unittest.TestCase):
    """Tests for the Supabase REST API integration."""
    
    def setUp(self):
        """Set up Supabase client before each test."""
        load_dotenv()
        self.supabase_url = os.environ.get("SUPABASE_URL")
        self.supabase_key = os.environ.get("SUPABASE_KEY")
        
        if not self.supabase_url or not self.supabase_key:
            self.skipTest("Supabase credentials not found in environment variables")
        
        self.supabase: Client = create_client(self.supabase_url, self.supabase_key)

    def test_user_favorites_query(self):
        """Test querying user favorites for a specific variant ID."""
        # The specific product ID to test
        test_variant_id = 439031072
        
        try:
            print(f"Querying Supabase for users who favorited variant_id: {test_variant_id}")
            
            # Query userfavorites table to find users who have favorited this variant
            response = self.supabase.table("userfavorites") \
                .select("user_id") \
                .eq("variant_id", test_variant_id) \
                .execute()
                
            # Extract user_ids from the response
            favorites_data = response.data
            user_ids = [item['user_id'] for item in favorites_data] if favorites_data else []
            
            print(f"Found {len(user_ids)} users who favorited this variant")
            
            # If we found users, get their emails and notification settings
            if user_ids:
                user_emails = []
                for user_id in user_ids:
                    # Get user profile
                    profile_response = self.supabase.table("profiles") \
                        .select("email, is_active") \
                        .eq("user_id", user_id) \
                        .execute()
                    
                    # Get notification settings
                    settings_response = self.supabase.table("usernotificationsettings") \
                        .select("notify_on_price_drop") \
                        .eq("user_id", user_id) \
                        .execute()
                    
                    # Check if user is active and has notifications enabled
                    profile_data = profile_response.data
                    settings_data = settings_response.data
                    
                    if profile_data and settings_data:
                        profile = profile_data[0]
                        settings = settings_data[0]
                        
                        if profile.get('is_active') and settings.get('notify_on_price_drop'):
                            user_emails.append(profile.get('email'))
                
                print(f"Found {len(user_emails)} subscribed users with active notifications")
                if user_emails:
                    print(f"Emails: {user_emails[:3]}")
            
            # Test should pass even if no users found
            self.assertIsNotNone(favorites_data)
            
        except Exception as e:
            self.fail(f"Supabase API query failed: {str(e)}")
    
    def test_connection(self):
        """Simple test to verify Supabase connection is working."""
        try:
            # Get current timestamp from Supabase
            response = self.supabase.rpc('get_timestamp').execute()
            print(f"Supabase connection successful: {response.data}")
            self.assertIsNotNone(response.data)
        except Exception as e:
            self.fail(f"Supabase connection failed: {str(e)}")


if __name__ == '__main__':
    unittest.main()