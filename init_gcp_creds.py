#!/usr/bin/env python3
import os
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("gcp-credentials-fixer")

def ensure_gcp_credentials():
    """Ensure GCP credentials are correctly configured in container"""
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        logger.error("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
        return False
        
    logger.info("Checking credentials at: " + creds_path)
    
    if not os.path.exists(creds_path):
        logger.error("Credentials file not found at " + creds_path)
        return False
    
    try:
        with open(creds_path, "r") as f:
            creds_data = json.load(f)
        
        required_keys = ["type", "project_id", "private_key_id", "private_key", "client_email"]
        missing_keys = [key for key in required_keys if key not in creds_data]
        
        if missing_keys:
            logger.error("Credentials file missing required fields: " + str(missing_keys))
            return False
            
        logger.info("Credentials for project " + creds_data.get("project_id") + " appear valid")
        
        # Create symlink in default location to handle potential library differences
        default_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        os.makedirs(os.path.dirname(default_path), exist_ok=True)
        
        if not os.path.exists(default_path):
            logger.info("Creating symlink from " + creds_path + " to " + default_path)
            os.symlink(creds_path, default_path)
        else:
            logger.info("Default credentials already exist at " + default_path)
        
        return True
    except json.JSONDecodeError:
        logger.error("Credentials file is not valid JSON")
        return False
    except Exception as e:
        logger.error("Error validating credentials: " + str(e))
        return False

if __name__ == "__main__":
    if ensure_gcp_credentials():
        print("✅ GCP credentials configured successfully")
        sys.exit(0)
    else:
        print("❌ Failed to configure GCP credentials")
        sys.exit(1)
