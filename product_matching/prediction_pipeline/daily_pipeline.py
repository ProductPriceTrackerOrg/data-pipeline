"""
Daily pipeline script for processing new products and finding matches
This is designed to be run by Airflow or similar scheduler
"""
import pandas as pd
import numpy as np
import logging
import sys
from datetime import datetime
from typing import List, Optional
from .product_matcher import ProductMatcher
from .model_loader import model_loader
from .config import *

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class DailyPipeline:
    """Daily pipeline for processing new products"""
    
    def __init__(self):
        self.matcher = ProductMatcher()
        self.model_loader = model_loader
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def load_new_products(self, csv_path: str) -> pd.DataFrame:
        """Load new products to be processed"""
        logger.info(f"Loading new products from: {csv_path}")
        
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            logger.error(f"New products file not found: {csv_path}")
            return pd.DataFrame()
        
        # Validate required columns
        required_columns = ['product_id', 'product_title']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Clean data
        df = df.dropna(subset=['product_title'])
        df['product_title'] = df['product_title'].astype(str).str.strip()
        df = df[df['product_title'] != '']
        
        logger.info(f"Loaded {len(df)} new products for processing")
        return df
    
    def process_new_products(self, new_products_df: pd.DataFrame) -> pd.DataFrame:
        """Process new products and find matches"""
        if len(new_products_df) == 0:
            logger.info("No new products to process")
            return pd.DataFrame(columns=['match_group_id', 'shop_product_id', 'confidence_score'])
        
        # Load artifacts
        self.matcher.load_artifacts()
        
        # Extract product information
        product_ids = new_products_df['product_id'].tolist()
        product_titles = new_products_df['product_title'].tolist()
        
        # Generate embeddings
        embeddings = self.model_loader.generate_embeddings(product_titles)
        
        # Find matches
        results = self.matcher.find_matches(embeddings, product_ids)
        
        # Convert to DataFrame
        results_df = self.matcher.results_to_dataframe(results)
        
        # Update index with new products
        self.matcher.update_index(embeddings, product_ids)
        
        return results_df
    
    
    def generate_summary_report(self, results_df: pd.DataFrame) -> dict:
        """Generate summary report of daily processing"""
        if len(results_df) == 0:
            return {
                'timestamp': self.run_timestamp,
                'new_products_processed': 0,
                'matches_found': 0,
                'new_groups_created': 0,
                'average_confidence': 0.0
            }
        
        # Calculate statistics
        total_products = len(results_df)
        matches_found = len(results_df[results_df['confidence_score'] < 1.0])
        new_groups = len(results_df[results_df['confidence_score'] == 1.0])
        avg_confidence = results_df['confidence_score'].mean()
        
        return {
            'timestamp': self.run_timestamp,
            'new_products_processed': total_products,
            'matches_found': matches_found,
            'new_groups_created': new_groups,
            'average_confidence': avg_confidence,
            'unique_groups_total': results_df['match_group_id'].nunique()
        }
    
    def run_daily_pipeline(self, new_products_csv: str) -> dict:
        """Run the complete daily pipeline"""
        logger.info("Starting daily pipeline execution")
        
        try:
            # Load new products
            new_products_df = self.load_new_products(new_products_csv)
            
            # Process products and find matches
            results_df = self.process_new_products(new_products_df)
            
            # Generate summary
            summary = self.generate_summary_report(results_df)
            
            logger.info("Daily pipeline completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Daily pipeline failed: {e}")
            raise

def main():
    """Main function for daily pipeline execution"""
    if len(sys.argv) < 2:
        print("Usage: python daily_pipeline.py <new_products_csv_path>")
        sys.exit(1)
    
    new_products_csv = sys.argv[1]
    
    try:
        pipeline = DailyPipeline()
        summary = pipeline.run_daily_pipeline(new_products_csv)
        
        # Print summary report
        print("\n" + "="*50)
        print("DAILY PIPELINE SUMMARY REPORT")
        print("="*50)
        print(f"Execution Time: {summary['timestamp']}")
        print(f"New Products Processed: {summary['new_products_processed']}")
        print(f"Matches Found: {summary['matches_found']}")
        print(f"New Groups Created: {summary['new_groups_created']}")
        print(f"Average Confidence: {summary['average_confidence']:.4f}")
        print(f"Total Unique Groups: {summary.get('unique_groups_total', 'N/A')}")
        print("="*50)
        
        print("Daily pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Daily pipeline execution failed: {e}")
        print(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()