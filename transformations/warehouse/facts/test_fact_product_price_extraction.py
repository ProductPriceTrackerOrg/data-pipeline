import unittest
import json
from unittest.mock import patch, mock_open
from fact_product_price import main

SAMPLE_LIST_JSON = json.dumps([
    {
        "product_id_native": "p1",
        "variants": [
            {
                "variant_id_native": "v1",
                "price_current": "1000",
                "price_original": "1200",
                "availability_text": "In Stock"
            }
        ],
        "metadata": {
            "source_website": "appleme.lk",
            "scrape_timestamp": "2025-09-03T04:55:13.412182"
        }
    }
])

SAMPLE_DICT_JSON = json.dumps({
    "data": [
        {
            "product_id_native": "p2",
            "variants": [
                {
                    "variant_id_native": "v2",
                    "price_current": "2000",
                    "price_original": "2200",
                    "availability_text": "In Stock"
                }
            ],
            "metadata": {
                "source_website": "appleme.lk",
                "scrape_timestamp": "2025-09-03T04:55:13.412182"
            }
        }
    ]
})

class TestFactProductPriceExtraction(unittest.TestCase):
    @patch("builtins.open", new_callable=mock_open, read_data=SAMPLE_LIST_JSON)
    @patch("fact_product_price.bigquery.Client")
    def test_extract_from_list(self, mock_client, mock_file):
        mock_client.return_value.query.return_value.result.return_value = [type("MockRow", (object,), {"next_id": 1})()]
        with patch("fact_product_price.print") as mock_print:
            main()
            mock_print.assert_any_call("Extracted 1 price fact rows.")

    @patch("builtins.open", new_callable=mock_open, read_data=SAMPLE_DICT_JSON)
    @patch("fact_product_price.bigquery.Client")
    def test_extract_from_dict(self, mock_client, mock_file):
        mock_client.return_value.query.return_value.result.return_value = [type("MockRow", (object,), {"next_id": 1})()]
        with patch("fact_product_price.print") as mock_print:
            main()
            mock_print.assert_any_call("Extracted 1 price fact rows.")

if __name__ == "__main__":
    unittest.main()
