# Onei.lk Web Scraper

A modular, production-ready web scraper for extracting product data from [Onei.lk](https://onei.lk) and uploading it to Azure Data Lake Storage.

## 🚀 Features

- **Modular Architecture**: Clean separation of concerns with organized folder structure
- **Azure Integration**: Automatic upload to Azure Data Lake Storage with proper partitioning
- **Error Handling**: Robust error handling with automatic JSON fixing capabilities
- **High Performance**: Optimized Scrapy spider with concurrent requests and caching
- **Data Validation**: Built-in JSON validation and data quality checks
- **Logging**: Comprehensive logging for debugging and monitoring

## 📁 Project Structure

```
Onei.lk/
├── main.py                     # Main entry point
├── oneiscraper.py             # Legacy single-file scraper (for reference)
├── fix_and_upload.py          # Utility to fix existing data and upload
├── .env                       # Environment variables (Azure credentials)
├── config/
│   └── scraper_config.py      # Configuration settings
├── models/
│   └── product_models.py      # Pydantic data models
├── scripts/
│   ├── product_scraper_core.py    # Core Scrapy spider logic
│   └── product_scraper_manager.py # Scraper orchestration
├── utils/
│   ├── __init__.py            # Utils package
│   └── scraper_utils.py       # Utility functions
├── test/
│   ├── debug_json.py          # JSON debugging tool
│   ├── fix_json.py            # JSON repair utility
│   └── test_upload.py         # Azure upload testing
└── README.md                  # This file
```

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Azure Storage Account with connection string

### Install Dependencies

```bash
pip install scrapy azure-storage-blob python-dotenv pydantic
```

### Environment Setup

1. Create a `.env` file in the project root:

```env
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=your_account;AccountKey=your_key;EndpointSuffix=core.windows.net"
```

2. Replace with your actual Azure Storage connection string.

## 🚀 Usage

### Basic Scraping and Upload

```bash
python main.py
```

This will:

1. Scrape all products from Onei.lk
2. Save data locally as `one1lk_products.json`
3. Upload to Azure Data Lake Storage as `data.json`

### Fix Existing Data and Upload

```bash
python fix_and_upload.py
```

This utility:

- Fixes variant titles to match product titles
- Repairs malformed JSON files
- Uploads clean data to Azure

### Testing Azure Connection

```bash
python test/test_upload.py
```

## 📊 Data Schema

### Product Structure

```json
{
  "product_id_native": "12345",
  "product_url": "https://onei.lk/product/example",
  "product_title": "iPhone 15 Pro",
  "description_html": "<div>Product description...</div>",
  "brand": "Apple",
  "category_path": ["Electronics", "Smartphones"],
  "image_urls": ["https://example.com/image1.jpg"],
  "variants": [
    {
      "variant_id_native": "12345-v1",
      "variant_title": "iPhone 15 Pro",
      "price_current": "250000.00",
      "price_original": "280000.00",
      "currency": "LKR",
      "availability_text": "In stock"
    }
  ],
  "metadata": {
    "source_website": "https://onei.lk",
    "shop_contact_phone": "+94770176666",
    "shop_contact_whatsapp": "+94770176666",
    "scrape_timestamp": "2025-09-03T15:24:49.123456"
  }
}
```

## ⚙️ Configuration

### Scraper Settings (`config/scraper_config.py`)

- **Concurrent Requests**: Configurable request concurrency
- **Delays**: Request delays and rate limiting
- **User Agents**: Rotating user agents for better success rates
- **Retry Logic**: Automatic retries for failed requests

### Azure Settings

- **Container**: `raw-data`
- **Path Pattern**: `source_website=onei.lk/scrape_date=YYYY-MM-DD/data.json`
- **Encoding**: UTF-8 with proper Unicode handling

## 🔧 Troubleshooting

### Common Issues

#### 1. Malformed JSON Error

```
JSONDecodeError: Expecting ',' delimiter
```

**Solution**: Run the JSON fixer

```bash
python test/debug_json.py
```

#### 2. Azure Connection Error

```
ValueError: Azure connection string not found
```

**Solution**: Check your `.env` file and ensure the connection string is correct.

#### 3. Empty Scraped Data

**Solution**: Check if the website structure changed or if rate limiting is active.

### Debug Tools

- **JSON Debugger**: `python test/debug_json.py`
- **Azure Test**: `python test/test_upload.py`
- **Fix Utility**: `python fix_and_upload.py`

## 📈 Performance

### Typical Performance Metrics

- **Speed**: ~2-5 products/second
- **Success Rate**: >95% under normal conditions
- **Data Size**: ~3.5MB for 1,284 products
- **Memory Usage**: ~100-200MB during execution

### Optimization Tips

1. Adjust `CONCURRENT_REQUESTS` in configuration
2. Modify `DOWNLOAD_DELAY` for rate limiting
3. Use caching for development testing
4. Monitor logs for bottlenecks

## 🔒 Data Quality

### Validation Checks

- ✅ JSON schema validation
- ✅ Required fields presence
- ✅ URL format validation
- ✅ Price format consistency
- ✅ UTF-8 encoding compliance

### Data Cleaning

- Variant titles automatically set to product titles
- HTML content properly escaped
- Duplicate image URLs removed
- Empty fields handled gracefully

## 📋 Azure Data Lake Structure

```
raw-data/
└── source_website=onei.lk/
    └── scrape_date=2025-09-03/
        └── data.json          # Clean, validated product data
```

## 🤝 Contributing

### Code Style

- Follow PEP 8 guidelines
- Use meaningful variable names
- Add docstrings to functions
- Include type hints where appropriate

### Testing

Before submitting changes:

1. Test scraping functionality
2. Verify Azure upload works
3. Run JSON validation
4. Check data quality

## 📞 Support

### Contacts

- **Shop Contact**: +94770176666 (Phone/WhatsApp)
- **Website**: [https://onei.lk](https://onei.lk)

### Logs

Check `onei_scraper.log` for detailed execution logs and error messages.

## 📄 License

This project is for educational and research purposes. Please respect the website's robots.txt and terms of service.

---

**Last Updated**: September 3, 2025  
**Version**: 2.0 (Modular Architecture)  
**Data Source**: [Onei.lk](https://onei.lk)
