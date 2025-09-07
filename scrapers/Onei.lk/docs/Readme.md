# Onei.lk Web Scraper - Data Pipeline

A production-ready web scraper for extracting product data from Onei.lk and uploading to Azure Data Lake Storage (ADLS). This project is part of a comprehensive data science pipeline with robust error handling, data validation, and automated upload capabilities.

## 🏗️ Project Architecture

```
Onei.lk/
├── .env                              # Environment variables (Azure credentials)
├── main.py                          # Main application entry point
├── README.md                        # This file
├── SUCCESS_SUMMARY.md               # Project success documentation
├── onei_scraper.log                 # Application logs
├── 
├── Core Application Files
├── ├── oneiscraper.py               # Core scraper implementation
├── ├── run_scraper.py               # Scraper execution script
├── ├── run_fresh_pipeline.py        # Fresh data pipeline runner
├── 
├── Data Processing & Fixes
├── ├── fix_and_run_fresh.py         # Fix data and run fresh pipeline
├── ├── fix_and_upload.py            # Fix existing data and upload
├── ├── fix_json.py                  # JSON data repair utilities
├── ├── final_fix.py                 # Final data cleaning script
├── ├── quick_fix.py                 # Quick data repair tool
├── 
├── Utilities & Debug Tools
├── ├── debug_json.py                # JSON debugging utilities
├── ├── debug_structure.py           # Data structure debugging
├── ├── simple_upload.py             # Simple Azure upload script
├── ├── verify_upload.py             # Upload verification tool
├── 
├── Configuration
├── config/
│   └── scraper_config.py           # Scraper configuration settings
├── 
├── Data Models
├── models/
│   └── product_models.py           # Pydantic data validation models
├── 
├── Core Scripts
├── scripts/
│   ├── product_scraper_core.py     # Core Scrapy spider
│   └── product_scraper_manager.py  # Scraping orchestration
├── 
├── Utilities
├── utils/
│   ├── __init__.py
│   └── scraper_utils.py            # Helper utilities
├── 
├── Testing
├── test/
│   ├── test_main.py                # Main function tests
│   ├── test_upload.py              # Upload functionality tests  
│   ├── test_load.py                # Data loading tests
│   ├── test_quality.py             # Data quality tests
│   ├── test_validation.py          # Validation tests
│   └── checkconnectdatalake/       # Azure connectivity tests
├── 

```

## ✨ Key Features

- **🔄 Fresh Data Pipeline**: Automated daily scraping with data refresh capabilities
- **☁️ Azure Integration**: Direct upload to Azure Data Lake Storage with partitioning
- **🛠️ Data Repair Tools**: Multiple utilities to fix and validate scraped data
- **📊 Data Quality Assurance**: Comprehensive validation using Pydantic models
- **🧪 Extensive Testing**: Unit tests for all major components
- **📈 Historical Data**: Maintains historical scraping records
- **🔍 Debug Tools**: Built-in debugging and structure analysis utilities
- **⚡ Performance Optimized**: Concurrent processing with intelligent caching

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Azure Storage Account with Data Lake Storage
- Internet connection

### Installation

1. **Navigate to project directory**
   ```bash
   cd "d:\My Campus Work\Sem 05\Projects\Data Science Project New\data-pipeline\scrapers\Onei.lk"
   ```

2. **Install dependencies**
   ```bash
   pip install scrapy pydantic azure-storage-blob python-dotenv requests
   ```

3. **Configure environment variables**
   Update `.env` file with your Azure credentials:
   ```env
   AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=your_account;AccountKey=your_key;EndpointSuffix=core.windows.net"
   ```

### 🎯 Usage Options

#### Option 1: Run Fresh Pipeline (Recommended)
```bash
python run_fresh_pipeline.py
```
*Clean scraping with immediate Azure upload*

#### Option 2: Run Standard Scraper
```bash
python run_scraper.py
```
*Standard scraping with local file output*

#### Option 3: Fix and Upload Existing Data
```bash
python fix_and_upload.py
```
*Repair and upload historical data files*

#### Option 4: Main Application
```bash
python main.py
```
*Complete pipeline with all features*

## 🧪 Testing Framework

### Run All Tests
```bash
# Run complete test suite
python -m pytest test/ -v

# Run specific test categories
python test_main.py           # Main functionality
python test_upload.py         # Azure upload tests
python test_quality.py        # Data quality tests
python test_validation.py     # Data validation tests
python test_load.py          # Data loading tests
```

### Test Coverage
- ✅ **Main Functions**: Core application logic
- ✅ **Azure Upload**: Storage connectivity and upload
- ✅ **Data Quality**: Structure and content validation
- ✅ **JSON Processing**: Data parsing and repair
- ✅ **Network Connectivity**: Website accessibility

## 🔧 Configuration

### Scraper Settings (`config/scraper_config.py`)
```python
# Performance Settings
CONCURRENT_REQUESTS = 16
DOWNLOAD_DELAY = 1.0
DOWNLOAD_TIMEOUT = 30
RETRY_TIMES = 3

# Output Settings
OUTPUT_FORMAT = 'json'
FEED_EXPORT_ENCODING = 'utf-8'

# Azure Settings
AZURE_CONTAINER = 'raw-data'
SOURCE_WEBSITE = 'onei.lk'
```

## 📊 Data Structure

### Product Model Schema
```json
{
  "title": "Product Name",
  "description": "Detailed product description",
  "brand": "Brand Name", 
  "category": "Product Category",
  "price": 25000.0,
  "original_price": 30000.0,
  "discount_percentage": 16.67,
  "currency": "LKR",
  "availability": "in_stock",
  "url": "https://onei.lk/product-url",
  "product_id": "unique-identifier",
  "images": [
    {
      "url": "image-url",
      "alt_text": "Image description", 
      "is_primary": true
    }
  ],
  "specifications": [...],
  "variants": [...],
  "scraped_at": "2025-01-XX T XX:XX:XX",
  "source_website": "onei.lk"
}
```

## 🛠️ Data Repair Tools

The project includes several specialized tools for data quality management:

### Fix Corrupted JSON Data
```bash
python fix_json.py          # Basic JSON repair
python final_fix.py         # Comprehensive data cleaning
python quick_fix.py         # Quick fixes for common issues
```

### Debug Data Issues
```bash
python debug_json.py        # Analyze JSON structure
python debug_structure.py   # Deep structure analysis
```

### Upload Management
```bash
python simple_upload.py     # Simple Azure upload
python verify_upload.py     # Verify uploaded data
```

## 📈 Historical Data Management

The project maintains historical scraping data with daily snapshots:
- `one1lk_products-2025.08.07.json` - August 7, 2025
- `one1lk_products2025-08-08.json` - August 8, 2025
- `one1lk_products2025-08-28.json` - August 28, 2025
- And more recent daily captures...

## 🔐 Security & Best Practices

### Environment Configuration
- ✅ Credentials stored in `.env` file
- ✅ Connection strings never hardcoded
- ✅ Azure Key Vault compatible
- ✅ Secure authentication patterns

### Data Privacy
- ✅ Public product data only
- ✅ No personal information collected
- ✅ Respectful crawling with delays
- ✅ Robot.txt compliance

## ⚡ Performance Features

- **Concurrent Processing**: 16 parallel requests
- **HTTP Caching**: Scrapy cache for development
- **Request Throttling**: 1-second delays between requests
- **Memory Efficiency**: Streaming data processing
- **Error Recovery**: Automatic retry mechanisms

## 📋 Dependencies

```txt
scrapy>=2.5.0
pydantic>=1.8.0  
azure-storage-blob>=12.0.0
python-dotenv>=0.19.0
requests>=2.25.0
```

## 🐛 Troubleshooting

### Common Issues & Solutions

1. **Azure Upload Failed**
   ```bash
   # Check connection
   python test/checkconnectdatalake/connectdatalake.py
   
   # Verify credentials
   python verify_upload.py
   ```

2. **Data Quality Issues**
   ```bash
   # Run quality checks
   python test_quality.py
   
   # Fix data issues
   python final_fix.py
   ```

3. **JSON Parsing Errors**
   ```bash
   # Debug JSON structure
   python debug_json.py
   
   # Repair corrupted files
   python fix_json.py
   ```

4. **Scraping Failures**
   ```bash
   # Check website connectivity
   python test_main.py
   
   # Run fresh pipeline
   python run_fresh_pipeline.py
   ```

### Debug Mode
Enable detailed logging by checking `onei_scraper.log` for error details.

## 📊 Success Metrics

See `SUCCESS_SUMMARY.md` for detailed project achievements and metrics.

## 🔄 Pipeline Workflow

1. **Initialize**: Load configuration and setup logging
2. **Scrape**: Extract fresh product data from Onei.lk
3. **Validate**: Check data quality and structure
4. **Clean**: Apply data cleaning and normalization
5. **Upload**: Push to Azure Data Lake Storage
6. **Verify**: Confirm successful upload
7. **Archive**: Store historical snapshots

## 🤝 Contributing

1. Run tests before making changes: `python -m pytest test/`
2. Follow existing code structure and patterns
3. Update documentation for new features
4. Ensure all data quality checks pass
5. Test Azure upload functionality

## 📞 Support

For issues and questions:
1. Check `onei_scraper.log` for error details
2. Run relevant test files to identify issues
3. Use debug tools to analyze data problems
4. Consult Azure documentation for storage issues

## 🏆 Project Status

✅ **Production Ready**  
✅ **Daily Data Collection**  
✅ **Azure Integration Active**  
✅ **Quality Assurance Implemented**  
✅ **Comprehensive Testing**  

---

**Last Updated**: January 2025  
**Data Source**: Onei.lk  
**Storage**: Azure Data Lake Storage  
**Framework**: Python + Scrapy + Pydantic  
**Status**: Active Production Pipeline