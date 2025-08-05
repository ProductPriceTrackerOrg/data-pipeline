# SimplyTek High-Speed Web Scraper

A high-performance, concurrent web scraper designed to collect product data from SimplyTek.lk with built-in rate limiting, error handling, and data validation.

## Features

- **High-Speed Concurrent Scraping**: Uses asyncio and aiohttp for maximum performance
- **Intelligent Rate Limiting**: Automatically handles HTTP 429 responses with exponential backoff
- **Data Validation**: Uses Pydantic models to ensure data integrity
- **Robust Error Handling**: Comprehensive retry logic and error reporting
- **Modular Architecture**: Clean separation of concerns across multiple files
- **Production Ready**: Includes logging, progress tracking, and comprehensive testing

## Quick Start

### Test the Scraper

```bash
python test_scraper.py
```

### Sample Scraping (10 products)

```bash
python main.py --sample --sample-size 10
```

### Full Production Scraping

```bash
python main.py
```

## Usage Examples

### Basic Usage

```python
from scraper import SimplyTekScraper
import asyncio

async def main():
    scraper = SimplyTekScraper()
    result = await scraper.scrape_all_products()
    scraper.save_results('products.json')

asyncio.run(main())
```

### Custom Configuration

```python
scraper = SimplyTekScraper(
    max_pages=10,           # Limit pages to scrape
    concurrent_requests=3   # Reduce concurrent requests
)
```

### Command Line Options

```bash
# Full scraping with custom settings
python main.py --max-pages 15 --concurrent-requests 3 --output my_products.json

# Sample scraping for testing
python main.py --sample --sample-size 20

# Debug mode with file logging
python main.py --log-level DEBUG --log-file scraper.log
```

## File Structure

```
├── models/           # Pydantic data models
├── scripts/
├── main.py             # Production runner
├── test_scraper.py     # Test suite
└── README.md          # This file
```

## Data Model

The scraper extracts the following data for each product:

```json
{
  "product_id_native": "8755076530495",
  "product_url": "https://www.simplytek.lk/products/aspor-charging-cable-2m",
  "product_title": "Aspor Charging Cable 1M / 2M - A100L/A102L/A101L/A101",
  "description_html": "<p>Length:- 1M/2M 3.0A...</p>",
  "brand": "Aspor",
  "category_path": ["aspor-charging-cable-2m"],
  "image_urls": ["https://www.simplytek.lk/cdn/shop/files/..."],
  "variants": [
    {
      "variant_id_native": "46624588071231",
      "variant_title": "Type-C A102L - 2M",
      "price_current": "Rs 499.00",
      "price_original": "Rs 1,199.00",
      "currency": "LKR",
      "availability_text": "In Stock"
    }
  ],
  "metadata": {
    "source_website": "simplytek.lk",
    "shop_contact_phone": "0117555888",
    "shop_contact_whatsapp": "+94 72 672 9729",
    "scrape_timestamp": "2025-01-XX"
  }
}
```

## Performance

- **Speed**: Scrapes ~500-1000 products in 2-5 minutes
- **Concurrent Requests**: Default 5 (configurable)
- **Rate Limiting**: Automatic handling of HTTP 429 responses
- **Memory Efficient**: Processes products in batches

## Error Handling

The scraper includes comprehensive error handling:

- **HTTP Errors**: Automatic retries with exponential backoff
- **Rate Limiting**: Intelligent delay increases on 429 responses
- **Parsing Errors**: Graceful handling of malformed HTML
- **Network Issues**: Timeout handling and connection pooling
- **Data Validation**: Pydantic model validation

## Rate Limiting Features

- Respects server rate limits with automatic delays
- Exponential backoff on 429 responses
- Jitter addition to prevent thundering herd
- User agent rotation
- Connection pooling for efficiency

## Testing

The test suite includes:

- Data validation testing
- Single product scraping test
- Collection page parsing test
- Rate limiting functionality test
- Sample scraping test
- Full scraping test (limited)

Run tests:

```bash
python test_scraper.py
```

## Configuration

### Environment Variables

```bash
export SCRAPER_MAX_PAGES=20
export SCRAPER_CONCURRENT_REQUESTS=3
export SCRAPER_DELAY=0.5
```

### Scraper Parameters

- `max_pages`: Maximum collection pages to scrape (default: 25)
- `concurrent_requests`: Number of simultaneous requests (default: 5)
- `delay_between_requests`: Delay between requests in seconds (default: 0.3)
- `max_retries`: Maximum retry attempts (default: 3)

## Troubleshooting

### Common Issues

1. **HTTP 429 Errors**

   - Reduce `concurrent_requests` to 2-3
   - Increase `delay_between_requests` to 0.5-1.0

2. **Slow Performance**

   - Check internet connection
   - Reduce batch size in scraper configuration
   - Monitor server response times

3. **Parsing Errors**

   - Website structure may have changed
   - Check sample HTML against parser logic
   - Update CSS selectors if needed

4. **Memory Issues**
   - Process products in smaller batches
   - Implement result streaming for large datasets

### Debug Mode

```bash
python main.py --log-level DEBUG --log-file debug.log
```

## Best Practices

1. **Start with Sample**: Always test with `--sample` first
2. **Monitor Rate Limits**: Watch for 429 responses in logs
3. **Use Reasonable Concurrency**: 3-5 concurrent requests recommended
4. **Save Regularly**: Results are saved automatically on completion
5. **Check Data Quality**: Validate scraped data before use

## Legal and Ethical Considerations

- Respect robots.txt and terms of service
- Use reasonable request rates
- Don't overload the target server
- Consider caching to reduce repeated requests
- Monitor for IP blocking or access restrictions

## License

This scraper is provided as-is for educational and legitimate business purposes. Users are responsible for complying with applicable laws and website terms of service.
