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
from big_query.data_insert import upload_matches_to_bigquery

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def run_backfill() -> bool:
    """Run backfill matching process"""
    try:
       
        print("Starting Backfill Matching...")
        print("-" * 50)

        unmatched_products_df = get_unmatched_products()
        
        backfill_matcher = BackfillMatcher()
        results_df = backfill_matcher.run_backfill(unmatched_products_df)
        upload_matches_to_bigquery(results_df)

        print(f"Backfill results ready with {len(results_df)} total matches")
        
        print("Backfill matching completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Backfill failed: {e}")
        print(f"Backfill failed: {e}")
        return False
    
if __name__ == "__main__":
    run_backfill()