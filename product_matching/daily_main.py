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

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def run_daily_pipeline() -> bool:
    """Run daily pipeline process"""
    try:
        
        print("Starting Daily Pipeline...")
        print("-" * 50)
        
        new_products_csv = get_unmatched_products()
        
        pipeline = DailyPipeline()
        summary = pipeline.run_daily_pipeline(new_products_csv)
        
        # Print detailed summary
        print("\nDaily Pipeline Summary:")
        print(f"   Timestamp: {summary['timestamp']}")
        print(f"   Products Processed: {summary['new_products_processed']}")
        print(f"   Matches Found: {summary['matches_found']}")
        print(f"   New Groups Created: {summary['new_groups_created']}")
        print(f"   Average Confidence: {summary['average_confidence']:.4f}")
        
        print("Daily pipeline completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}")
        print(f"Daily pipeline failed: {e}")
        return False