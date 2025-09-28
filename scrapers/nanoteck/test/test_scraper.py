"""
Test Scraper Module
Unit tests for scraper components and Azure connectivity.
"""

import json
import os
import sys
import unittest
from unittest.mock import Mock, patch
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

# Import Azure modules with fallback
try:
    from azure.storage.blob import BlobServiceClient
    from azure.core.exceptions import AzureError

    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    print("⚠️  Azure SDK not available. Install with: pip install azure-storage-blob")

# Import config with fallback
try:
    from config.scraper_config import AZURE_CONFIG
except ImportError:
    # Fallback Azure config
    AZURE_CONFIG = {"container_name": "raw-data", "enable_upload": True}


class TestAzureConnectivity(unittest.TestCase):
    """Test Azure Data Lake Storage connectivity."""

    def setUp(self):
        """Set up test environment."""
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = AZURE_CONFIG.get("container_name", "raw-data")
        self.test_blob_name = (
            f"test/connectivity_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

    def test_azure_sdk_available(self):
        """Test if Azure SDK is properly installed."""
        self.assertTrue(
            AZURE_AVAILABLE,
            "Azure SDK is not installed. Run: pip install azure-storage-blob",
        )

    def test_connection_string_exists(self):
        """Test if Azure connection string is configured."""
        self.assertIsNotNone(
            self.connection_string,
            "AZURE_STORAGE_CONNECTION_STRING not found in environment variables. Please add it to your .env file.",
        )
        self.assertNotEqual(
            self.connection_string.strip(),
            "",
            "AZURE_STORAGE_CONNECTION_STRING is empty.",
        )

    @unittest.skipIf(not AZURE_AVAILABLE, "Azure SDK not available")
    def test_azure_connection(self):
        """Test Azure blob service connection."""
        if not self.connection_string:
            self.skipTest("Azure connection string not configured")

        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )

            # Test connection by listing containers (lightweight operation)
            containers = []
            container_iter = blob_service_client.list_containers()
            count = 0
            for container in container_iter:
                containers.append(container)
                count += 1
                if count >= 1:  # Just get one container
                    break

            self.assertIsInstance(containers, list, "Failed to list containers")

            print("✅ Azure connection successful")

        except AzureError as e:
            self.fail(f"❌ Azure connection failed: {e}")
        except Exception as e:
            self.fail(f"❌ Unexpected error during Azure connection test: {e}")

    @unittest.skipIf(not AZURE_AVAILABLE, "Azure SDK not available")
    def test_container_exists_or_create(self):
        """Test if container exists or can be created."""
        if not self.connection_string:
            self.skipTest("Azure connection string not configured")

        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            container_client = blob_service_client.get_container_client(
                self.container_name
            )

            # Check if container exists
            try:
                properties = container_client.get_container_properties()
                print(f"✅ Container '{self.container_name}' exists and is accessible")
                self.assertIsNotNone(
                    properties, "Container properties should not be None"
                )

            except AzureError as e:
                if "ContainerNotFound" in str(e):
                    print(f"⚠️  Container '{self.container_name}' does not exist")
                    # Try to create container (this might fail due to permissions)
                    try:
                        container_client.create_container()
                        print(
                            f"✅ Container '{self.container_name}' created successfully"
                        )
                    except AzureError as create_error:
                        self.fail(
                            f"❌ Cannot create container '{self.container_name}': {create_error}"
                        )
                else:
                    self.fail(f"❌ Error accessing container: {e}")

        except Exception as e:
            self.fail(f"❌ Unexpected error during container test: {e}")

    @unittest.skipIf(not AZURE_AVAILABLE, "Azure SDK not available")
    def test_upload_download_blob(self):
        """Test blob upload and download functionality."""
        if not self.connection_string:
            self.skipTest("Azure connection string not configured")

        test_data = {
            "test_message": "Azure connectivity test",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "nanotek_scraper_test",
        }
        test_json = json.dumps(test_data, indent=2)

        try:
            blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, blob=self.test_blob_name
            )

            # Upload test data
            blob_client.upload_blob(
                test_json,
                overwrite=True,
                metadata={
                    "test_type": "connectivity_test",
                    "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    "scraper_version": "2.0",
                },
            )
            print(f"✅ Test blob uploaded: {self.test_blob_name}")

            # Download and verify
            downloaded_data = blob_client.download_blob().readall().decode("utf-8")
            downloaded_json = json.loads(downloaded_data)

            self.assertEqual(
                downloaded_json["test_message"],
                test_data["test_message"],
                "Downloaded data doesn't match uploaded data",
            )

            # Get blob properties
            properties = blob_client.get_blob_properties()
            self.assertIsNotNone(
                properties.metadata, "Blob metadata should not be None"
            )
            self.assertEqual(
                properties.metadata.get("test_type"),
                "connectivity_test",
                "Metadata not preserved",
            )

            print(f"✅ Test blob downloaded and verified successfully")
            print(f"   Blob size: {properties.size} bytes")
            print(f"   Last modified: {properties.last_modified}")

            # Clean up test blob
            blob_client.delete_blob()
            print(f"✅ Test blob cleaned up: {self.test_blob_name}")

        except AzureError as e:
            self.fail(f"❌ Azure blob operation failed: {e}")
        except Exception as e:
            self.fail(f"❌ Unexpected error during blob test: {e}")

    def test_azure_configuration(self):
        """Test Azure configuration settings."""
        self.assertIsInstance(AZURE_CONFIG, dict, "AZURE_CONFIG should be a dictionary")
        self.assertIn(
            "container_name", AZURE_CONFIG, "container_name should be in AZURE_CONFIG"
        )
        self.assertIn(
            "enable_upload", AZURE_CONFIG, "enable_upload should be in AZURE_CONFIG"
        )

        container_name = AZURE_CONFIG.get("container_name")
        self.assertIsInstance(container_name, str, "container_name should be a string")
        self.assertNotEqual(
            container_name.strip(), "", "container_name should not be empty"
        )

        print(f"✅ Azure configuration valid:")
        print(f"   Container: {container_name}")
        print(f"   Upload enabled: {AZURE_CONFIG.get('enable_upload')}")


class TestProductScraperCore(unittest.TestCase):
    """Test cases for ProductScraperCore."""

    def setUp(self):
        # Mock config and logger since we don't have the actual classes yet
        self.config = Mock()
        self.config.base_url = "https://www.nanotek.lk"
        self.config.max_images_per_product = 5
        self.config.max_spec_tables = 2
        self.config.max_specs_per_table = 10

        self.logger = Mock()

    def test_image_url_filtering(self):
        """Test image URL filtering for valid extensions."""
        # Mock the extract_product_data method behavior
        from urllib.parse import urljoin

        html = """
        <div class="ty-sequence-slider">
            <img src="image1.jpg">
            <img src="image2.webp">
            <img src="image3.png">
            <img src="image4.jpeg">
            <img src="image5.svg">
            <img src="image6.gif">
            <img src="data:image/png;base64,abc123">
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")

        # Simulate image URL extraction logic
        image_urls = []
        base_url = "https://www.nanotek.lk"
        image_elements = soup.select(".ty-sequence-slider img")

        for img in image_elements[:5]:  # max_images_per_product
            src = img.get("src")
            if src and "data:" not in src:
                full_url = urljoin(base_url, src)
                # Check for valid image extensions
                if any(
                    full_url.lower().endswith(ext)
                    for ext in [".jpg", ".jpeg", ".png", ".webp"]
                ):
                    image_urls.append(full_url)

        # Should only include .jpg, .jpeg, .webp, .png (4 valid images)
        self.assertEqual(len(image_urls), 4)
        self.assertTrue(any("image1.jpg" in url for url in image_urls))
        self.assertTrue(any("image2.webp" in url for url in image_urls))
        self.assertTrue(any("image3.png" in url for url in image_urls))
        self.assertTrue(any("image4.jpeg" in url for url in image_urls))

        # Should NOT include .svg, .gif, or data URLs
        self.assertFalse(any("image5.svg" in url for url in image_urls))
        self.assertFalse(any("image6.gif" in url for url in image_urls))
        self.assertFalse(any("data:image" in url for url in image_urls))

        print(f"✅ Image filtering test passed: {len(image_urls)} valid images found")


def run_azure_tests():
    """Run only Azure connectivity tests."""
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestAzureConnectivity))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


def run_all_tests():
    """Run all tests."""
    unittest.main(verbosity=2)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--azure-only":
        # Run only Azure tests
        print("🧪 Running Azure connectivity tests only...\n")
        success = run_azure_tests()
        if success:
            print("\n✅ All Azure tests passed!")
            sys.exit(0)
        else:
            print("\n❌ Some Azure tests failed!")
            sys.exit(1)
    else:
        # Run all tests
        print("🧪 Running all tests...\n")
        unittest.main(verbosity=2)
