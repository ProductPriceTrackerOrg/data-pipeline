"""
Email service module for sending price change notifications.
This module is responsible for sending emails via SendGrid API.
"""

import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def send_notification_email(user_email: str, product: dict) -> bool:
    """
    Sends a price alert email using the SendGrid API.
    
    Args:
        user_email (str): The recipient's email address.
        product (dict): A dictionary containing product details.
        
    This function:
    1. Loads the SendGrid API key and sender email from environment variables.
    2. Constructs a user-friendly HTML email body.
    3. Formats prices with proper currency symbol.
    4. Sends the email using SendGrid API.
    
    Returns:
        bool: True if the email was sent successfully, False otherwise
    """
    # Get the API Key and sender email from environment variables
    sendgrid_api_key = os.getenv("SENDGRID_API_KEY")
    sender_email = os.getenv("SENDER_EMAIL", "alerts@pricepulse.lk") 
    
    if not sendgrid_api_key:
        print(f"Error: SENDGRID_API_KEY environment variable not set. Cannot send email to {user_email}.")
        return False
    
    # --- Dynamic Content Generation ---
    new_price = product.get('new_price', 0)
    old_price = product.get('old_price', 0)
    product_title = product.get('product_title_native', 'Product')
    product_url = product.get('product_url', '#')
    
    # Determine price change direction
    if new_price < old_price:
        change_type = "dropped"
        price_change_description = "Price Drop Alert"
        price_change_style = "color: green; font-weight: bold;"
    else:
        change_type = "increased"
        price_change_description = "Price Increase Alert"
        price_change_style = "color: red; font-weight: bold;"
    
    # Calculate price difference and percentage
    price_diff = abs(new_price - old_price)
    price_diff_percent = (price_diff / old_price) * 100 if old_price > 0 else 0
    
    # Create email subject line
    subject = f"Price {change_type.capitalize()}! {product_title} - LKR {price_diff:,.2f} ({price_diff_percent:.1f}%)"
    
    # Create HTML email content
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
            .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
            .header {{ background-color: #4285F4; color: white; padding: 10px 20px; text-align: center; }}
            .product-title {{ font-size: 18px; font-weight: bold; margin: 15px 0; }}
            .price-change {{ {price_change_style} }}
            .price-details {{ background-color: #f9f9f9; padding: 15px; margin: 15px 0; border-radius: 5px; }}
            .button {{ display: inline-block; padding: 10px 20px; background-color: #4285F4; color: white; text-decoration: none; border-radius: 5px; }}
            .footer {{ margin-top: 20px; font-size: 12px; color: #666; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>PricePulse {price_change_description}</h2>
            </div>
            
            <p>Hello,</p>
            <p>We've detected a price change for a product you're tracking:</p>
            
            <div class="product-title">{product_title}</div>
            
            <div class="price-details">
                <p>Old Price: LKR {old_price:,.2f}</p>
                <p class="price-change">New Price: LKR {new_price:,.2f}</p>
                <p>Difference: <span class="price-change">LKR {price_diff:,.2f} ({price_diff_percent:.1f}%)</span></p>
            </div>
            
            <p>
                <a href="{product_url}" class="button">View Product</a>
            </p>
            
            <div class="footer">
                <p>This is an automated notification from PricePulse.</p>
                <p>© 2025 PricePulse. All rights reserved.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Create the SendGrid Mail object
    message = Mail(
        from_email=sender_email,
        to_emails=user_email,
        subject=subject,
        html_content=html_content
    )
    
    # Send the email
    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        
        # Log the outcome for debugging
        print(f"Email sent to {user_email} for product '{product_title}' (variant_id: {product['variant_id']})")
        print(f"SendGrid Status Code: {response.status_code}")
        return True
        
    except Exception as e:
        print(f"Error sending email to {user_email}: {str(e)}")
        return False