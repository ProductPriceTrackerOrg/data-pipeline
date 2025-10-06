"""
Integration test for SendGrid email service.
"""

import unittest
from dotenv import load_dotenv
import os

class TestSendgridIntegration(unittest.TestCase):
    """Tests for the SendGrid email integration."""
    
    def setUp(self):
        """Set up environment before each test."""
        load_dotenv()
        self.api_key = os.getenv("SENDGRID_API_KEY")
        self.sender_email = os.getenv("SENDER_EMAIL")
        
        if not self.api_key or not self.sender_email:
            self.skipTest("SendGrid credentials not found in environment variables")
    
    def test_send_real_email(self):
        """Test sending an actual email using SendGrid API."""
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail
            
            recipient_email = "anjanapraneeth7@gmail.com"  # Use your email for testing
            
            # Create test email content
            subject = "PricePulse Test Email"
            html_content = """
            <html>
            <body>
                <h2>PricePulse Test Email</h2>
                <p>This is a test email from the PricePulse notification system.</p>
                <p>If you receive this email, the SendGrid integration is working correctly.</p>
            </body>
            </html>
            """
            
            # Create the email message
            message = Mail(
                from_email=self.sender_email,
                to_emails=recipient_email,
                subject=subject,
                html_content=html_content
            )
            
            # Send the email
            sg = SendGridAPIClient(self.api_key)
            response = sg.send(message)
            
            print(f"Email sent with status code: {response.status_code}")
            self.assertIn(response.status_code, [200, 201, 202])
            
        except Exception as e:
            self.fail(f"Failed to send email: {str(e)}")

if __name__ == '__main__':
    unittest.main()