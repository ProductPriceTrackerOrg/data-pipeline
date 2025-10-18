#!/usr/bin/env python3
"""
Download a sample product page from laptop.lk for testing price extraction
"""
import httpx
import os
import sys
import time

def download_sample_page(url=None):
    """Download a sample page from laptop.lk for testing"""
    # Use the provided URL or default to a monitor product with known price issues
    if not url:
        # URL for a monitor product with price discrepancy
        url = "https://www.laptop.lk/index.php/product/viewsonic-27-ips-monitor/"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0"
    }
    
    try:
        # Download the page with a retry mechanism
        for attempt in range(3):
            try:
                print(f"Attempt {attempt+1}: Downloading {url}")
                response = httpx.get(url, headers=headers, follow_redirects=True, timeout=30)
                response.raise_for_status()
                html_content = response.text
                break
            except Exception as e:
                print(f"Attempt {attempt+1} failed: {e}")
                if attempt == 2:  # Last attempt
                    raise
                time.sleep(2)  # Wait before retrying
        
        # Save to a file in the current directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "temp_webpage.html")
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        
        print(f"Successfully downloaded and saved page to {output_path}")
        print(f"Content length: {len(html_content)} bytes")
        print(f"URL: {url}")
        
        return output_path
    
    except Exception as e:
        print(f"Error downloading sample page: {e}")
        return None

if __name__ == "__main__":
    # Accept a URL as command line argument if provided
    import sys
    url = sys.argv[1] if len(sys.argv) > 1 else None
    download_sample_page(url)