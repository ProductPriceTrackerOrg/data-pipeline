"""
Updated PostgreSQL module for querying user notification subscriptions using Supabase API.
This module is responsible for finding users who are subscribed to product price notifications.
"""

from typing import List, Dict
import os
from supabase import create_client, Client

def get_subscribed_users_supabase(variant_id: int) -> List[str]:
    """
    Queries a Supabase database to find all users subscribed to a given variant_id.
    
    Args:
        variant_id (int): The ID of the variant to check for.
        
    This function:
    1. Uses the Supabase REST API to query the database
    2. Filters by the provided variant_id
    3. Ensures the user has notifications enabled
    4. Returns a list of user email strings
    
    Returns:
        List[str]: List of email addresses for users subscribed to the variant
    """
    print(f"Finding subscribed users for variant_id: {variant_id} using Supabase API...")
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("Error: Missing Supabase credentials in environment variables")
        return []
    
    try:
        # Initialize the Supabase client
        supabase: Client = create_client(supabase_url, supabase_key)
        
        # First, get the user_ids who have favorited this variant
        favorites_response = supabase.table("userfavorites") \
            .select("user_id") \
            .eq("variant_id", variant_id) \
            .execute()
            
        if not favorites_response.data:
            print(f"No users have favorited variant_id: {variant_id}")
            return []
            
        user_ids = [item['user_id'] for item in favorites_response.data]
        print(f"Found {len(user_ids)} users who favorited this variant")
        
        # Collect emails of users with notifications enabled
        user_emails = []
        for user_id in user_ids:
            # Get user profile data and notification settings in separate queries
            profile_response = supabase.from_("profiles") \
                .select("email, is_active") \
                .eq("user_id", user_id) \
                .execute()
                
            settings_response = supabase.from_("usernotificationsettings") \
                .select("notify_on_price_drop") \
                .eq("user_id", user_id) \
                .execute()
                
            # Check if the user is active and has notifications enabled
            if profile_response.data and settings_response.data:
                profile = profile_response.data[0]
                settings = settings_response.data[0]
                
                if profile.get('is_active') and settings.get('notify_on_price_drop'):
                    user_emails.append(profile.get('email'))
        
        print(f"Found {len(user_emails)} subscribed users with active notifications")
        return user_emails
        
    except Exception as e:
        print(f"Error querying Supabase for subscribed users: {str(e)}")
        return []

# Keep the original function for backward compatibility
def get_subscribed_users(variant_id: int, conn=None) -> List[str]:
    """
    Legacy function that delegates to the Supabase implementation.
    
    Args:
        variant_id (int): The ID of the variant to check for.
        conn: Ignored - kept for backward compatibility.
        
    Returns:
        List[str]: List of email addresses for users subscribed to the variant
    """
    return get_subscribed_users_supabase(variant_id)