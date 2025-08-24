#!/usr/bin/env python3
"""
Test script to check if Brotli is correctly installed and working
"""
import sys
import platform

def check_brotli():
    """Check if Brotli is available and print the version"""
    try:
        import brotli
        print(f"Brotli is installed. Version: {brotli.__version__}")
        return True
    except ImportError:
        print("Brotli is NOT installed")
        return False

def check_aiohttp():
    """Check if aiohttp is available and print the version"""
    try:
        import aiohttp
        print(f"aiohttp is installed. Version: {aiohttp.__version__}")
        
        try:
            from aiohttp import _http_writer
            print("aiohttp C accelerator is available")
        except ImportError:
            print("WARNING: aiohttp C accelerator is NOT available")
            
        return True
    except ImportError:
        print("aiohttp is NOT installed")
        return False

def print_system_info():
    """Print system information"""
    print(f"Python version: {sys.version}")
    print(f"Platform: {platform.platform()}")
    print(f"Machine: {platform.machine()}")
    print(f"Architecture: {platform.architecture()}")

if __name__ == "__main__":
    print("\n=== System Information ===")
    print_system_info()
    
    print("\n=== Brotli Check ===")
    check_brotli()
    
    print("\n=== aiohttp Check ===")
    check_aiohttp()
    
    print("\nTest completed.")
