# Laptop.lk Scraper

A high-performance asynchronous web scraper for [Laptop.lk](https://www.laptop.lk) that collects product data and stores it both locally and in Azure Data Lake Storage.

## Features

- Asynchronous HTTP requests for fast data collection
- XML sitemap-based URL discovery
- Efficient HTML parsing with Selectolax
- Progress tracking with ETA
- Azure Data Lake Storage integration
- Local file storage with JSON format

## Requirements

- Python 3.7+
- Required packages:
  - httpx
  - selectolax
  - azure-storage-blob
  - python-dotenv
  - asyncio

## Setup

1. Install the required dependencies:

```bash
pip install httpx selectolax azure-storage-blob python-dotenv
```

2. This scraper uses the `.env` file from the root of the project, which should already contain the Azure Storage connection string:

```
AZURE_STORAGE_CONNECTION_STRING=your_connection_string_here
```

If this environment variable is not present in the root `.env` file, please add it.

## Usage

To run the scraper, simply execute:

```bash
python main.py
```

This will:
1. Extract product URLs from the Laptop.lk sitemap
2. Scrape product details from each URL
3. Save the data locally in the `scraped_data` directory
4. Upload the data to Azure Data Lake Storage

## Data Format

The scraped data is stored in JSON format with the following structure:

```json
[
  {
    "product_id_native": "12345",
    "product_url": "https://www.laptop.lk/product/example",
    "product_title": "Example Laptop",
    "warranty": "1 Year Warranty",
    "description_html": "<p>Product description</p>",
    "brand": "HP",
    "category_path": ["Laptops", "Gaming Laptops"],
    "image_urls": ["https://www.laptop.lk/wp-content/uploads/image1.jpg"],
    "variants": [
      {
        "variant_id_native": "12345",
        "variant_title": "Default",
        "price_current": "299999",
        "price_original": "349999",
        "currency": "LKR",
        "availability_text": "In Stock"
      }
    ],
    "metadata": {
      "source_website": "laptop.lk",
      "shop_contact_phone": "+94 77 733 6464",
      "shop_contact_whatsapp": "+94 77 733 6464",
      "scrape_timestamp": "2025-09-11T12:00:00"
    }
  }
]
```

## Azure Data Lake Storage

Data is uploaded to Azure Data Lake Storage with Hive-style partitioning:

```
raw-data/source_website=laptop.lk/scrape_date=2025-09-11/data.json
```

This allows for efficient querying based on source website and scrape date.
