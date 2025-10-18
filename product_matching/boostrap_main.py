import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

from fastapi import logger

# Import all pipeline components
from prediction_pipeline.bootstrap_index import IndexBootstrapper
from prediction_pipeline.backfill_matches import BackfillMatcher
from prediction_pipeline.daily_pipeline import DailyPipeline
from prediction_pipeline.inference_utils import InferenceUtils
from prediction_pipeline.config import *
from big_query.data_loading import get_unmatched_products


def run_bootstrap() -> bool:
    """Run bootstrap process"""
    try:
        print("Starting Bootstrap Process...")
        print("-" * 50)

        unmatched_products_df = get_unmatched_products()

        if not os.path.exists(SIAMESE_MODEL_PATH):
            print(f"Siamese model not found: {SIAMESE_MODEL_PATH}")
            print("Please ensure you have trained the model first.")
            return False
        
        bootstrapper = IndexBootstrapper()
        index, product_ids = bootstrapper.bootstrap(unmatched_products_df)
        
        print("Bootstrap completed successfully!")
        print(f"   Index: {FAISS_INDEX_PATH}")
        print(f"   ID Map: {PRODUCT_ID_MAP_PATH}")
        print(f"   Products indexed: {len(product_ids)}")
        
        return True
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        print(f"Bootstrap failed: {e}")
        return False
    
if __name__ == "__main__":
    run_bootstrap()