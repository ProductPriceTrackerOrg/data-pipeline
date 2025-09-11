#!/usr/bin/env python3
"""
Main pipeline runner for ADLS extraction to BigQuery staging
End-to-end execution script
"""
import os
import sys
import logging
from datetime import datetime, timedelta
import argparse

# Add transformations to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'transformations'))

from extraction.azure_data_lake.extractor import ADLSExtractor
from loading.bigquery.loader import BigQueryLoader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'transformations/logs/pipeline_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

def run_extraction_pipeline(date: str, sources: list = None):
    """Run complete extraction pipeline for a date"""
    logger.info(f"🚀 Starting ADLS → BigQuery pipeline for {date}")
    
    try:
        # Step 1: Initialize components
        logger.info("📝 Step 1: Initializing pipeline components...")
        extractor = ADLSExtractor()
        loader = BigQueryLoader()
        
        # Step 2: Extract from ADLS
        logger.info("📥 Step 2: Extracting from ADLS...")
        extracted_data = extractor.extract_for_date(date, sources)
        
        if not extracted_data:
            logger.error("❌ No data extracted from ADLS")
            return False
        
        # Save extraction results for debugging
        extractor.save_extraction_results(extracted_data, date)
        
        # Step 3: Load to BigQuery
        logger.info("📤 Step 3: Loading to BigQuery staging...")
        load_results = loader.load_multiple_sources(extracted_data, date)
        
        # Step 4: Validate loads
        logger.info("✅ Step 4: Validating loaded data...")
        validation_results = {}
        
        for source in load_results.keys():
            if load_results[source] > 0:
                validation = loader.validate_load(source, date)
                validation_results[source] = validation
                
                # Get sample products for verification
                samples = loader.get_sample_products(source, date, 3)
                logger.info(f"📋 {source} samples: {len(samples)} products")
        
        # Step 5: Generate summary
        total_extracted = sum(len(products) for products in extracted_data.values())
        total_loaded = sum(load_results.values())
        successful_sources = len([r for r in load_results.values() if r > 0])
        
        logger.info(f"🎯 Pipeline Summary for {date}:")
        logger.info(f"   Sources discovered: {len(extracted_data)}")
        logger.info(f"   Sources successful: {successful_sources}")
        logger.info(f"   Products extracted: {total_extracted}")
        logger.info(f"   Rows loaded to BigQuery: {total_loaded}")
        
        # Detailed results
        for source in extracted_data.keys():
            extracted_count = len(extracted_data[source])
            loaded_count = load_results.get(source, 0)
            status = "✅" if loaded_count > 0 else "❌"
            logger.info(f"   {status} {source}: {extracted_count} → {loaded_count}")
        
        # Success criteria
        success = (
            total_loaded > 0 and
            successful_sources > 0 and
            total_loaded == total_extracted
        )
        
        if success:
            logger.info("🎉 Pipeline completed successfully!")
        else:
            logger.warning("⚠️ Pipeline completed with issues")
        
        return success
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        return False

def run_test_pipeline():
    """Run pipeline with test/sample data"""
    logger.info("🧪 Running test pipeline...")
    
    # Use yesterday's date for testing (adjust as needed)
    test_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    return run_extraction_pipeline(test_date)

def run_backfill(start_date: str, end_date: str):
    """Run pipeline for a date range (backfill)"""
    logger.info(f"🔄 Running backfill from {start_date} to {end_date}")
    
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    results = []
    current_date = start
    
    while current_date <= end:
        date_str = current_date.strftime('%Y-%m-%d')
        logger.info(f"\n📅 Processing {date_str}...")
        
        success = run_extraction_pipeline(date_str)
        results.append((date_str, success))
        
        current_date += timedelta(days=1)
    
    # Backfill summary
    successful_days = len([r for r in results if r[1]])
    total_days = len(results)
    
    logger.info(f"\n🏁 Backfill Summary:")
    logger.info(f"   Date range: {start_date} to {end_date}")
    logger.info(f"   Days processed: {total_days}")
    logger.info(f"   Days successful: {successful_days}")
    logger.info(f"   Success rate: {(successful_days/total_days)*100:.1f}%")
    
    # Show failed dates
    failed_dates = [date for date, success in results if not success]
    if failed_dates:
        logger.warning(f"   Failed dates: {failed_dates}")
    
    return successful_days == total_days

def main():
    """Main entry point with command line arguments"""
    parser = argparse.ArgumentParser(description='ADLS to BigQuery Pipeline')
    parser.add_argument('--date', type=str, help='Date to process (YYYY-MM-DD)', 
                       default=datetime.now().strftime('%Y-%m-%d'))
    parser.add_argument('--sources', nargs='*', help='Specific sources to process')
    parser.add_argument('--test', action='store_true', help='Run test pipeline')
    parser.add_argument('--backfill', nargs=2, metavar=('START', 'END'), 
                       help='Backfill date range (YYYY-MM-DD YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        if args.test:
            success = run_test_pipeline()
        elif args.backfill:
            success = run_backfill(args.backfill[0], args.backfill[1])
        else:
            success = run_extraction_pipeline(args.date, args.sources)
        
        if success:
            logger.info("✅ Pipeline execution completed successfully!")
            exit(0)
        else:
            logger.error("❌ Pipeline execution failed!")
            exit(1)
            
    except KeyboardInterrupt:
        logger.info("⚠️ Pipeline interrupted by user")
        exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
