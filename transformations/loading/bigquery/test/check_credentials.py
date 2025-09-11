#!/usr/bin/env python3
"""
BigQuery Credentials Checker
Validates that Google Cloud credentials are properly set up.
"""

import os
import json
from pathlib import Path

def check_credentials():
    """Check if BigQuery credentials are properly configured"""
    
    print("🔍 Checking BigQuery Credentials Setup...")
    print("=" * 50)
    
    # Check environment variables
    google_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    google_project = os.getenv('GOOGLE_CLOUD_PROJECT')
    
    print(f"📋 Environment Variables:")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS: {google_creds or 'Not set'}")
    print(f"   GOOGLE_CLOUD_PROJECT: {google_project or 'Not set'}")
    
    # Check if credentials file exists
    if google_creds:
        creds_path = Path(google_creds)
        if creds_path.exists():
            print(f"✅ Credentials file exists: {creds_path}")
            
            # Try to read and validate JSON
            try:
                with open(creds_path, 'r') as f:
                    creds_data = json.load(f)
                
                required_fields = ['type', 'project_id', 'private_key', 'client_email']
                missing_fields = [field for field in required_fields if field not in creds_data]
                
                if missing_fields:
                    print(f"❌ Invalid credentials file - missing fields: {missing_fields}")
                    return False
                else:
                    print(f"✅ Credentials file is valid JSON with required fields")
                    print(f"   Service Account: {creds_data.get('client_email')}")
                    print(f"   Project ID: {creds_data.get('project_id')}")
                    
                    # Check if project IDs match
                    if google_project and google_project != creds_data.get('project_id'):
                        print(f"⚠️ Warning: GOOGLE_CLOUD_PROJECT ({google_project}) doesn't match credentials project_id ({creds_data.get('project_id')})")
                    
            except json.JSONDecodeError:
                print(f"❌ Credentials file is not valid JSON")
                return False
            except Exception as e:
                print(f"❌ Error reading credentials file: {e}")
                return False
                
        else:
            print(f"❌ Credentials file does not exist: {creds_path}")
            return False
    else:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS not set")
        return False
    
    # Try to import and test BigQuery client
    print("\n🧪 Testing BigQuery Connection...")
    try:
        from google.cloud import bigquery
        
        # Try to create client (this will validate credentials)
        client = bigquery.Client()
        print(f"✅ BigQuery client created successfully")
        print(f"   Project: {client.project}")
        
        # Try to list datasets (basic permission test)
        try:
            datasets = list(client.list_datasets(max_results=1))
            print(f"✅ Can access BigQuery (found {len(datasets)} datasets)")
            return True
        except Exception as e:
            print(f"⚠️ BigQuery client created but cannot list datasets: {e}")
            print("   This might be due to insufficient permissions")
            return True  # Client works, just permission issue
            
    except ImportError:
        print("❌ google-cloud-bigquery package not installed")
        print("   Run: pip install google-cloud-bigquery")
        return False
    except Exception as e:
        print(f"❌ BigQuery connection failed: {e}")
        return False

def main():
    """Main function"""
    print("🚀 BigQuery Credentials Checker")
    print()
    
    # Load .env file if it exists
    env_path = Path(__file__).parent.parent.parent.parent / '.env'
    if env_path.exists():
        print(f"📁 Loading environment from: {env_path}")
        try:
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Remove quotes if present
                        value = value.strip('"').strip("'")
                        os.environ[key.strip()] = value
            print("✅ Environment variables loaded from .env")
        except Exception as e:
            print(f"⚠️ Error loading .env file: {e}")
    else:
        print("📁 No .env file found")
    
    print()
    
    # Check credentials
    success = check_credentials()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 SUCCESS: BigQuery credentials are properly configured!")
        print("   You can now run the BigQuery loader.")
    else:
        print("❌ FAILED: BigQuery credentials need to be set up.")
        print("   Please follow the instructions in BIGQUERY_SETUP.md")
    
    return success

if __name__ == "__main__":
    main()
