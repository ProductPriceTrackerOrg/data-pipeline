"""
Main orchestration module for the price notification service.
This module coordinates the process of finding price changes, finding subscribed users,
and sending notification emails.
"""

import os
import sys
from dotenv import load_dotenv  # Make sure to install: pip install python-dotenv
import logging
from datetime import datetime

# Import the functions from the other modules
from notification_service.bigquery_queries import get_price_changes
from notification_service.supabase_queries import get_subscribed_users_supabase as get_subscribed_users
from notification_service.email_service import send_notification_email


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"price_notification_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('price_notification')

def run_notification_service():
    """
    Main function to orchestrate the entire notification process.
    This is the entry point that will be called by the Airflow DAG.
    
    This function:
    1. Gets price changes from BigQuery data warehouse
    2. Uses Supabase REST API for user subscription data
    3. For each price change, finds subscribed users
    4. For each subscribed user, sends an email notification
    5. Logs the process details
    """
    logger.info("Starting price change notification service...")
    
    try:
        # Step 1: Get price changes from BigQuery
        price_changes = get_price_changes()
        
        if not price_changes:
            logger.info("No price changes detected. Service finished.")
            return
        
        logger.info(f"Found {len(price_changes)} products with price changes")
        
        # Step 2: We use Supabase REST API, no need for direct database connection
        logger.info("Using Supabase REST API for user subscription data")
        
        # Step 3: Process each price change
        emails_sent = 0
        total_users_found = 0
        
        for product in price_changes:
            variant_id = product['variant_id']
            shop_product_id = product.get('shop_product_id')
            logger.info(f"Processing variant {variant_id} for shop product {shop_product_id}...")
            
            # Step 4: Get users who have favorited this product and want notifications
            subscribed_users = get_subscribed_users(variant_id)
            total_users_found += len(subscribed_users)
            
            # Step 5: Send emails to each subscribed user
            if subscribed_users:
                logger.info(f"Found {len(subscribed_users)} subscribed users for variant {variant_id}")
                for email in subscribed_users:
                    success = send_notification_email(email, product)
                    if success:
                        emails_sent += 1
            else:
                logger.info(f"No subscribed users found for variant {variant_id}")
        
        # Summary statistics
        logger.info(f"Notification service summary: {emails_sent} emails sent to {total_users_found} users for {len(price_changes)} products")

    except Exception as e:
        logger.error(f"An error occurred during the notification process: {str(e)}", exc_info=True)
    
    logger.info("Price change notification service completed")

if __name__ == "__main__":
    # This allows the script to be run directly for testing
    load_dotenv()
    
    # Set up the credentials path if not already set
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        credentials_path = os.path.abspath("gcp-credentials.json")
        if os.path.exists(credentials_path):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
            print(f"Set GOOGLE_APPLICATION_CREDENTIALS to {credentials_path}")
        else:
            print(f"Warning: Credentials file not found at {credentials_path}")
    
    run_notification_service()