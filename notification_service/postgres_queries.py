"""
PostgreSQL module for querying user notification subscriptions.
This module is responsible for finding users who are subscribed to product price notifications.
"""

from typing import List, Dict

def get_subscribed_users(variant_id: int, conn) -> List[str]:
    """
    Queries a PostgreSQL database to find all users subscribed to a given variant_id.
    
    Args:
        variant_id (int): The ID of the variant to check for.
        conn: An active psycopg2 database connection object.
        
    This function:
    1. Defines a SQL query that JOINs the relevant tables
    2. Filters by the provided variant_id
    3. Ensures the user has notifications enabled
    4. Returns a list of user email strings
    
    Returns:
        List[str]: List of email addresses for users subscribed to the variant
    """
    print(f"Finding subscribed users for variant_id: {variant_id}...")
    
    # SQL query to find users who have favorited this variant and have notifications enabled
    # IMPORTANT: In the operational database, variant_id is actually product_id as per requirements
    query = """
    SELECT p.email FROM profiles p
    JOIN userfavorites f ON p.user_id = f.user_id
    JOIN usernotificationsettings s ON p.user_id = s.user_id
    WHERE f.variant_id = %s 
      AND s.notify_on_price_drop = TRUE
      AND p.is_active = TRUE;
    """
    
    emails = []
    try:
        with conn.cursor() as cur:
            cur.execute(query, (variant_id,))
            results = cur.fetchall()
            if results:
                emails = [row[0] for row in results]
                
        print(f"Found {len(emails)} subscribed users for variant_id {variant_id}")
        return emails
    except Exception as e:
        print(f"Error querying PostgreSQL for subscribed users: {str(e)}")
        return []