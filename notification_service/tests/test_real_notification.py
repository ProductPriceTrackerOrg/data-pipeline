"""
Test script for sending a real price change notification email.
This script bypasses the BigQuery query and directly sends an email
for a specific product variant ID.
"""

import os
from dotenv import load_dotenv
from notification_service.supabase_queries import get_subscribed_users_supabase
from notification_service.email_service import send_notification_email

def test_notification_for_specific_variant():
    """Send a test notification for a specific product variant."""
    # Load environment variables
    load_dotenv()
    
    # Specific variant ID to test with
    variant_id = 439031072
    
    print(f"Getting subscribed users for variant_id: {variant_id}")
    subscribed_users = get_subscribed_users_supabase(variant_id)
    
    print(f"Found {len(subscribed_users)} subscribed users")
    
    if not subscribed_users:
        # If no subscribed users found, use a test email
        subscribed_users = ["anjanapraneeth7@gmail.com"]
        print(f"No subscribed users found, using test email: {subscribed_users[0]}")
    
    # Create a test product object
    product = {
        "variant_id": variant_id,
        "product_title_native": "Test Product from Notification System",
        "old_price": 150000.00,
        "new_price": 125000.00,
        "product_url": "https://pricepulse.lk/products/439031072"
    }
    
    # Send notification to each subscribed user
    sent_count = 0
    for email in subscribed_users:
        print(f"Sending notification to: {email}")
        success = send_notification_email(email, product)
        if success:
            sent_count += 1
    
    print(f"Successfully sent {sent_count} notifications")

if __name__ == "__main__":
    test_notification_for_specific_variant()