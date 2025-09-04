# CyberDeals Scraper

A high-performance asynchronous scraper for CyberDeals.lk e-commerce website with Azure Data Lake Storage integration.

## Features

- Asynchronous HTTP requests with concurrency control
- Batch processing to handle large numbers of products
- Robust error handling and retry logic
- Comprehensive brand detection
- Multiple fallback strategies for data extraction
- **Azure Data Lake Storage (ADLS) integration**
- Partitioned data storage by source and date

## Installation

1. Install dependencies from the global requirements file:
```bash
# From the data-pipeline root directory
pip install -r requirements.txt
```

2. Environment Configuration:
   - The scraper uses the global `.env` file located in the data-pipeline root directory
   - Make sure `AZURE_STORAGE_CONNECTION_STRING` is configured in the global `.env` file
   - No additional environment setup required for this scraper

## Usage

```python
from scrapers.cyberdeals.main import run

# Run the scraper
run()
```

Or run directly:
```bash
python main.py
```

## Data Storage

### Azure Data Lake Storage (Primary)
- **Container**: `raw-data`
- **Path Pattern**: `source_website=cyberdeals/scrape_date=YYYY-MM-DD/data.json`
- **Format**: JSON with complete product information

### Local Storage (Optional)
- **Directory**: `scraped_data/`
- **File**: `cyberdeals_lk_scrape_optimized.json`

## Configuration

Configuration parameters can be modified in `config/scraper_config.py`.

## Output Data Structure

```json
{
  "product_id_native": "12345",
  "product_title": "Product Name",
  "brand": "Brand Name",
  "price_current": "1000.00",
  "currency": "LKR",
  "category_path": ["Category1", "Category2"],
  "image_urls": ["url1", "url2"],
  "metadata": {
    "source_website": "cyberdeals.lk",
    "scrape_timestamp": "2025-09-03T10:30:00"
  }
}
```
