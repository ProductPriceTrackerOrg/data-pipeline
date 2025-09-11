#!/usr/bin/env python3
"""
Show service account details to help with permission assignment
"""
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

def show_service_account_info():
    """Show service account information"""
    creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
    
    print(f"🔍 Service Account Information")
    print(f"=" * 50)
    print(f"📁 Credentials file: {creds_path}")
    print(f"🎯 Project ID: {project_id}")
    
    if creds_path and Path(creds_path).exists():
        try:
            with open(creds_path, 'r') as f:
                creds_data = json.load(f)
            
            print(f"\n📋 Service Account Details:")
            print(f"   Email: {creds_data.get('client_email')}")
            print(f"   Type: {creds_data.get('type')}")
            print(f"   Project ID: {creds_data.get('project_id')}")
            
            service_email = creds_data.get('client_email')
            
            print(f"\n🎯 ACTION REQUIRED:")
            print(f"=" * 50)
            print(f"1. Go to: https://console.cloud.google.com/iam-admin/iam")
            print(f"2. Make sure you're in project: {project_id}")
            print(f"3. Find this service account: {service_email}")
            print(f"4. Click the pencil icon to edit")
            print(f"5. Add role: 'BigQuery Admin'")
            print(f"6. Click Save")
            
            print(f"\n⚠️  IMPORTANT:")
            print(f"   - Edit the SERVICE ACCOUNT permissions (ends with .iam.gserviceaccount.com)")
            print(f"   - NOT your personal Google account")
            
        except Exception as e:
            print(f"❌ Error reading credentials: {e}")
    else:
        print(f"❌ Credentials file not found")

if __name__ == "__main__":
    show_service_account_info()
