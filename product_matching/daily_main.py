"""
Main script for daily product matching pipeline
Integrates BigQuery data loading and result insertion
"""
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Import pipeline components
from prediction_pipeline.daily_pipeline import DailyPipeline
from prediction_pipeline.config import *
from big_query.data_loading import get_unmatched_products
from big_query.data_insert import upload_matches_to_bigquery

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def run_daily_pipeline() -> bool:
    """Run daily pipeline process with BigQuery integration"""
    try:
        print("="*60)
        print("DAILY PRODUCT MATCHING PIPELINE")
        print("="*60)
        
        # Step 1: Load unmatched products from BigQuery
        print("\n[Step 1/4] Loading unmatched products from BigQuery...")
        print("-" * 60)
        new_products_df = get_unmatched_products()
        
        if new_products_df.empty:
            print("No unmatched products found. Nothing to process.")
            return True
        
        print(f"Loaded {len(new_products_df)} unmatched products")
        print(f"   Sample products:")
        print(new_products_df.head(3).to_string(index=False))
        
        # Step 2: Run the matching pipeline
        print("\n[Step 2/4] Running product matching pipeline...")
        print("-" * 60)
        pipeline = DailyPipeline()
        results_df, summary = pipeline.run_daily_pipeline(new_products_df)
        
        print(f"Pipeline completed successfully")
        
        # Step 3: Display summary
        print("\n[Step 3/4] Pipeline Summary:")
        print("-" * 60)
        print(f"   Timestamp: {summary['timestamp']}")
        print(f"   Products Processed: {summary['new_products_processed']}")
        print(f"   Matches Found: {summary['matches_found']}")
        print(f"   New Groups Created: {summary['new_groups_created']}")
        print(f"   Average Confidence: {summary['average_confidence']:.4f}")
        print(f"   Total Unique Groups: {summary.get('unique_groups_total', 'N/A')}")
        
        # Step 4: Upload results to BigQuery
        if not results_df.empty:
            print("\n[Step 4/4] Uploading results to BigQuery...")
            print("-" * 60)
            print(f"   Results DataFrame shape: {results_df.shape}")
            print(f"   Columns: {list(results_df.columns)}")
            print(f"\n   Sample results:")
            print(results_df.head(3).to_string(index=False))
            
            upload_matches_to_bigquery(results_df)
            print("Results uploaded successfully to FactProductMatch table")
        else:
            print("\n[Step 4/4] No results to upload")
        
        print("\n" + "="*60)
        print("DAILY PIPELINE COMPLETED SUCCESSFULLY!")
        print("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"Daily pipeline failed: {e}", exc_info=True)
        print(f"\nDaily pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    success = run_daily_pipeline()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()