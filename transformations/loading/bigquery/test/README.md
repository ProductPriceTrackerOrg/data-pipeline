# BigQuery Test Files Documentation

This folder contains all test and debugging scripts for the BigQuery loading functionality.

## 🧪 Test Files Overview

### Authentication & Connection Tests
- **`check_credentials.py`** - Verify BigQuery credentials setup
- **`debug_connection.py`** - Debug connection issues to BigQuery
- **`test_default_auth.py`** - Test gcloud default authentication
- **`show_service_account.py`** - Display service account information
- **`test_permissions.py`** - Test BigQuery permissions

### Dataset & Table Tests
- **`check_dataset.py`** - Verify staging dataset exists
- **`create_dataset.py`** - Create staging dataset if missing
- **`test_table_creation.py`** - Test table creation with 7-day partition retention

### Loading & Data Tests
- **`test_loader_dryrun.py`** - Dry run test of the main loader
- **`test_loading.py`** - Test actual data loading functionality
- **`explore_data.py`** - Explore loaded data structure and content

- **`test_overwrite_behavior.py`** - **🆕 Test immediate data overwrite and 1-day retention**

### Data Viewing Scripts
- **`final_viewer.py`** - **⭐ Main data viewer** - Shows real product data from all sources
- **`simple_viewer.py`** - Simple data viewing script
- **`view_data.py`** - Basic data viewing functionality
- **`show_products.py`** - Display product information

## 🎯 Key Test Results

✅ **1-Day Partition Retention:** Data automatically deleted after 1 day  
✅ **Immediate Overwrite:** New data replaces old data immediately  
✅ **Authentication:** gcloud default credentials working  
✅ **Data Loading:** 3,931 products loaded across 3 sources  
✅ **BigQuery Tables:** All staging tables created with proper partitioning  

## 🚀 Quick Test Commands

Run from the bigquery folder:

```bash
# Test authentication
python test/test_default_auth.py

# Test the new overwrite behavior
python test/test_overwrite_behavior.py

# View your data
python test/final_viewer.py

# Test table creation
python test/test_table_creation.py

# Dry run the loader
python test/test_loader_dryrun.py
```

## 📊 Production Files (Main Directory)

- **`loader.py`** - Main BigQuery loading functionality
- **`staging_schemas.py`** - BigQuery table schemas and structure
