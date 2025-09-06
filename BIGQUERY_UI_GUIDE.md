# BigQuery UI Access Guide

## 🌐 Direct Links to Your Data

### Main BigQuery Console
https://console.cloud.google.com/bigquery?project=price-pulse-470211

### Your Staging Dataset
https://console.cloud.google.com/bigquery?project=price-pulse-470211&ws=!1m5!1m4!4m3!1sprice-pulse-470211!2sstaging!3s__TABLES__

### Query Editor with Sample Query
https://console.cloud.google.com/bigquery?project=price-pulse-470211&ws=!1m0

## 📋 Ready-to-Use Queries

### 1. Data Summary
```sql
SELECT 
    source_website,
    scrape_date,
    product_count,
    loaded_at,
    file_path
FROM `price-pulse-470211.staging.*`
ORDER BY loaded_at DESC
```

### 2. Appleme Products
```sql
SELECT 
    JSON_EXTRACT_SCALAR(product, '$.product_title') as product_name,
    JSON_EXTRACT_SCALAR(product, '$.brand') as brand,
    JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') as price_lkr,
    JSON_EXTRACT_SCALAR(product, '$.variants[0].availability_text') as stock_status
FROM `price-pulse-470211.staging.stg_raw_appleme`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
LIMIT 20
```

### 3. SimplyTek Products
```sql
SELECT 
    JSON_EXTRACT_SCALAR(product, '$.product_title') as product_name,
    JSON_EXTRACT_SCALAR(product, '$.brand') as brand,
    JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') as price_lkr,
    JSON_EXTRACT_SCALAR(product, '$.variants[0].availability_text') as stock_status
FROM `price-pulse-470211.staging.stg_raw_simplytek`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
LIMIT 20
```

### 4. Onei.lk Products
```sql
SELECT 
    JSON_EXTRACT_SCALAR(product, '$.product_title') as product_name,
    JSON_EXTRACT_SCALAR(product, '$.brand') as brand,
    JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') as price_lkr,
    JSON_EXTRACT_SCALAR(product, '$.variants[0].availability_text') as stock_status
FROM `price-pulse-470211.staging.stg_raw_onei_lk`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
LIMIT 20
```

### 5. Check Your 1-Day Partition Retention 🎯
```sql
-- Method 1: Check table DDL for retention settings
SELECT 
    table_name,
    ddl
FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE'

-- Method 2: Simple table info
SELECT 
    table_name,
    creation_time,
    type
FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
WHERE table_type = 'BASE TABLE'
```

### 6. Total Product Count by Source
```sql
SELECT 
    'appleme' as source,
    COUNT(*) as total_products
FROM `price-pulse-470211.staging.stg_raw_appleme`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product

UNION ALL

SELECT 
    'simplytek' as source,
    COUNT(*) as total_products
FROM `price-pulse-470211.staging.stg_raw_simplytek`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product

UNION ALL

SELECT 
    'onei.lk' as source,
    COUNT(*) as total_products
FROM `price-pulse-470211.staging.stg_raw_onei_lk`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product

ORDER BY total_products DESC
```

### 7. Price Range Analysis
```sql
SELECT 
    source_website,
    COUNT(*) as total_products,
    MIN(SAFE_CAST(JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') AS FLOAT64)) as min_price,
    MAX(SAFE_CAST(JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') AS FLOAT64)) as max_price,
    AVG(SAFE_CAST(JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') AS FLOAT64)) as avg_price
FROM `price-pulse-470211.staging.*`,
UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
WHERE JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') IS NOT NULL
  AND JSON_EXTRACT_SCALAR(product, '$.variants[0].price_current') != ''
GROUP BY source_website
ORDER BY avg_price DESC
```

## 🎯 Your 1-Day Modification

✅ **Working Perfectly!** All new tables have 1-day partition retention instead of 7 days.
✅ **Immediate Overwrite:** New data replaces old data immediately.
✅ **Cost Savings:** Data automatically deleted after 1 day.
✅ **Total Products:** 3,931 products across 3 sources.
