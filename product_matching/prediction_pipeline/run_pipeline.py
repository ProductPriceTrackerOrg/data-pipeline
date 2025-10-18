"""
Master pipeline runner to execute the complete workflow
"""
import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Import all pipeline components
from .bootstrap_index import IndexBootstrapper
from .backfill_matches import BackfillMatcher
from .daily_pipeline import DailyPipeline
from .inference_utils import InferenceUtils
from .config import *

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class PipelineRunner:
    """Master pipeline runner for all operations"""
    
    def __init__(self):
        self.ensure_directories()
    
    def ensure_directories(self):
        """Ensure all required directories exist"""
        directories = [MODEL_DIR, INDEX_DIR, LOGS_DIR, DATA_DIR]
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def check_prerequisites(self, operation: str) -> bool:
        """Check if prerequisites exist for given operation"""
        if operation in ['backfill', 'daily', 'inference']:
            # Check if bootstrap has been run
            if not os.path.exists(FAISS_INDEX_PATH) or not os.path.exists(PRODUCT_ID_MAP_PATH):
                print("Bootstrap not completed. Run bootstrap first.")
                return False
            
            if not os.path.exists(SIAMESE_MODEL_PATH):
                print(f"Siamese model not found: {SIAMESE_MODEL_PATH}")
                return False
        
        return True
    
    def run_bootstrap(self, products_csv: str) -> bool:
        """Run bootstrap process"""
        try:
            print("Starting Bootstrap Process...")
            print("-" * 50)
            
            if not os.path.exists(products_csv):
                print(f"Products CSV not found: {products_csv}")
                return False
            
            if not os.path.exists(SIAMESE_MODEL_PATH):
                print(f"Siamese model not found: {SIAMESE_MODEL_PATH}")
                print("Please ensure you have trained the model first.")
                return False
            
            bootstrapper = IndexBootstrapper()
            index, product_ids = bootstrapper.bootstrap(products_csv)
            
            print("Bootstrap completed successfully!")
            print(f"   Index: {FAISS_INDEX_PATH}")
            print(f"   ID Map: {PRODUCT_ID_MAP_PATH}")
            print(f"   Products indexed: {len(product_ids)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Bootstrap failed: {e}")
            print(f"Bootstrap failed: {e}")
            return False
    
    def run_backfill(self, products_csv: str = None) -> bool:
        """Run backfill matching process"""
        try:
            if not self.check_prerequisites('backfill'):
                return False
            
            print("Starting Backfill Matching...")
            print("-" * 50)
            
            backfill_matcher = BackfillMatcher()
            results_df = backfill_matcher.run_backfill()
            # backfill_matcher.save_results(results_df)
            print(f"Backfill results ready with {len(results_df)} total matches")
            
            print("Backfill matching completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Backfill failed: {e}")
            print(f"Backfill failed: {e}")
            return False
    
    def run_daily_pipeline(self, new_products_csv: str) -> bool:
        """Run daily pipeline process"""
        try:
            if not self.check_prerequisites('daily'):
                return False
            
            print("Starting Daily Pipeline...")
            print("-" * 50)
            
            if not os.path.exists(new_products_csv):
                print(f"New products CSV not found: {new_products_csv}")
                return False
            
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
    
    def run_inference_demo(self) -> bool:
        """Run inference demonstration"""
        try:
            if not self.check_prerequisites('inference'):
                return False
            
            print("Running Inference Demo...")
            print("-" * 50)
            
            inference = InferenceUtils()
            
            # Validate index
            validation = inference.validate_index_consistency()
            print(f"Index Validation: {'Consistent' if validation['is_consistent'] else '⚠️ Inconsistent'}")
            
            # Get statistics
            stats = inference.get_index_statistics()
            print(f"Total Products in Index: {stats['total_products']}")
            print(f"Embedding Dimension: {stats['index_dimension']}")
            
            # Demo similarity search
            test_queries = [
                "iPhone 13 Pro Max",
                "Samsung Galaxy smartphone",
                "Nike running shoes"
            ]
            
            print("\nSimilarity Search Demo:")
            for query in test_queries:
                print(f"\nQuery: '{query}'")
                similar_products = inference.find_similar_products(query, top_k=3)
                
                if similar_products:
                    for i, product in enumerate(similar_products, 1):
                        print(f"  {i}. Product ID: {product['product_id']} "
                              f"(Confidence: {product['confidence_score']:.4f})")
                else:
                    print("  No similar products found")
            
            print("\nInference demo completed!")
            return True
            
        except Exception as e:
            logger.error(f"Inference demo failed: {e}")
            print(f"Inference demo failed: {e}")
            return False
    
    def show_status(self):
        """Show current pipeline status"""
        print("Pipeline Status Check")
        print("-" * 50)
        
        # Check model file
        if os.path.exists(SIAMESE_MODEL_PATH):
            print("Siamese Model: Found")
        else:
            print(f"Siamese Model: Not found ({SIAMESE_MODEL_PATH})")
        
        # Check index files
        if os.path.exists(FAISS_INDEX_PATH):
            print("FAISS Index: Found")
        else:
            print(f"FAISS Index: Not found ({FAISS_INDEX_PATH})")
        
        if os.path.exists(PRODUCT_ID_MAP_PATH):
            print("Product ID Map: Found")
        else:
            print(f"Product ID Map: Not found ({PRODUCT_ID_MAP_PATH})")
        
        # Check results file
        if os.path.exists(RESULTS_CSV_PATH):
            print("Results File: Found")
        else:
            print("Results File: Not found (will be created)")
        
        # Show index statistics if available
        try:
            inference = InferenceUtils()
            stats = inference.get_index_statistics()
            print(f"\nIndex Statistics:")
            print(f"   Total Products: {stats['total_products']}")
            print(f"   Embedding Dimension: {stats['index_dimension']}")
            print(f"   Index Type: {stats['index_type']}")
        except:
            print("Index Statistics: Not available")

def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="Product Matching Pipeline Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py bootstrap products.csv
  python run_pipeline.py backfill
  python run_pipeline.py daily new_products.csv
  python run_pipeline.py inference-demo
  python run_pipeline.py status
        """
    )
    
    parser.add_argument(
        'operation',
        choices=['bootstrap', 'backfill', 'daily', 'inference-demo', 'status'],
        help='Operation to perform'
    )
    
    parser.add_argument(
        'file',
        nargs='?',
        help='Input CSV file (required for bootstrap and daily operations)'
    )
    
    args = parser.parse_args()
    
    # Initialize runner
    runner = PipelineRunner()
    
    # Execute requested operation
    if args.operation == 'bootstrap':
        if not args.file:
            print("Products CSV file required for bootstrap operation")
            sys.exit(1)
        success = runner.run_bootstrap(args.file)
        
    elif args.operation == 'backfill':
        success = runner.run_backfill(args.file)
        
    elif args.operation == 'daily':
        if not args.file:
            print("New products CSV file required for daily operation")
            sys.exit(1)
        success = runner.run_daily_pipeline(args.file)
        
    elif args.operation == 'inference-demo':
        success = runner.run_inference_demo()
        
    elif args.operation == 'status':
        runner.show_status()
        success = True
    
    else:
        parser.print_help()
        sys.exit(1)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()