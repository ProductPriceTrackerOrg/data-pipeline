# BigQuery Staging Data Loader

## 🎯 **Purpose**

Loads raw scraped product data from Azure Data Lake Storage (ADLS) into BigQuery staging tables with **immediate overwrite** behavior.

## ⚡ **Key Features**

### **Immediate Overwrite Behavior**
- ✅ **New data immediately replaces old data**
- ✅ **Multiple runs per day = only latest run's data exists**
- ✅ **Staging tables act as temporary holding area**
- ✅ **No historical accumulation in staging**

### **1-Day Partition Retention**
- ✅ **Data automatically deleted after 1 day**
- ✅ **Cost optimization for staging tables**
- ✅ **Prevents staging table bloat**

### **Production Features**
- ✅ **Direct ADLS to BigQuery loading**
- ✅ **JSON array storage (1 row per website per load)**
- ✅ **Automatic table creation with partitioning**
- ✅ **Comprehensive error handling and logging**

## 📁 **File Structure**

```
transformations/loading/bigquery/
├── loader.py                 # 🎯 Main loading functionality
├── staging_schemas.py        # 📋 Table schemas and structure
├── test/                     # 🧪 All test files
│   ├── test_overwrite_behavior.py  # Test immediate overwrite
│   ├── final_viewer.py            # View loaded data
│   └── ... (13 more test files)
└── README.md                 # 📖 This documentation
```

## 🔧 **How It Works**

### **Data Flow:**
1. **Extract:** Scrapers save data to ADLS (`source_website=X/scrape_date=Y/data.json`)
2. **Load:** Loader reads from ADLS and loads to BigQuery staging
3. **Overwrite:** Each new load **deletes existing data** for that source
4. **Transform:** Downstream processes transform staging data to final tables

### **Staging Table Structure:**
```sql
CREATE TABLE staging.stg_raw_appleme (
    raw_json_data STRING,           -- Full JSON array of products
    scrape_date DATE,               -- Partition field
    source_website STRING,          -- Clustering field
    loaded_at TIMESTAMP,
    file_path STRING,
    product_count INT64
)
PARTITION BY scrape_date
CLUSTER BY source_website
OPTIONS (
    partition_expiration_days = 1   -- 🎯 1-day retention
)
```

## 🚀 **Usage**

### **Basic Usage:**
```python
from loader import BigQueryLoader

loader = BigQueryLoader()

# Load from ADLS (production method)
loader.load_from_adls_blob(
    source="appleme",
    scrape_date="2025-09-06"
)
```

## 🎯 **Overwrite Behavior Examples**

### **Scenario 1: Multiple Runs Per Day**
```bash
# 9:00 AM - Load 100 products
loader.load_from_adls_blob("appleme", "2025-09-06")
# Result: 100 products in staging

# 2:00 PM - Load 150 products (same day)
loader.load_from_adls_blob("appleme", "2025-09-06") 
# Result: 150 products in staging (100 deleted, 150 added)
```

### **Scenario 2: Daily Runs**
```bash
# Day 1 - Load appleme
loader.load_from_adls_blob("appleme", "2025-09-06")
# Result: Day 1 data in staging

# Day 2 - Load appleme
loader.load_from_adls_blob("appleme", "2025-09-07")
# Result: Day 2 data + Day 1 data (different partitions)

# Day 3 - Load appleme  
loader.load_from_adls_blob("appleme", "2025-09-08")
# Result: Day 3 + Day 2 data (Day 1 auto-deleted due to 1-day retention)
```

## 🧪 **Testing**

See the `test/` folder for all test files and documentation.

### **Quick Test Commands:**
```bash
# Test the new overwrite behavior
python test/test_overwrite_behavior.py

# View loaded data
python test/final_viewer.py

# Test authentication
python test/test_default_auth.py
```

## ⚙️ **Configuration**

### **Environment Variables:**
```bash
# Azure Storage
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_ACCOUNT_KEY=your_storage_key
AZURE_CONTAINER_NAME=scraped-data

# BigQuery (uses gcloud default credentials)
# No additional config needed if gcloud auth application-default login is set
```

## 🎯 **Benefits of This Design**

1. **Cost Effective:** 1-day retention saves storage costs
2. **Simple ETL:** Staging always has latest data only
3. **No Duplicates:** Overwrite prevents accumulation
4. **Fast Queries:** Less data = faster analysis
5. **Clear State:** Always know what's current
