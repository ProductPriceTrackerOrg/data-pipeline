"""
Daily pipeline script for processing new products and finding matches
This is designed to be run by Airflow or similar scheduler
"""
import pandas as pd
import numpy as np
import logging
import sys
from datetime import datetime
from typing import List, Optional, Tuple
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
    
    def load_new_products(self, new_products_df: pd.DataFrame) -> pd.DataFrame:
        """Load and validate new products DataFrame"""
        logger.info(f"Validating {len(new_products_df)} new products")
        
        # Validate required columns
        required_columns = ['product_id', 'product_title']
        missing_columns = [col for col in required_columns if col not in new_products_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Clean data
        df = new_products_df.copy()
        df = df.dropna(subset=['product_title'])
        df['product_title'] = df['product_title'].astype(str).str.strip()
        df = df[df['product_title'] != '']
        
        logger.info(f"Validated {len(df)} products for processing")
        return df
    
    def process_new_products(self, new_products_df: pd.DataFrame) -> pd.DataFrame:
        """Process new products and find matches"""
        if len(new_products_df) == 0:
            logger.info("No new products to process")
            return pd.DataFrame(columns=['match_group_id', 'shop_product_id', 'confidence_score'])
        
        # Load artifacts
        logger.info("Loading FAISS index and model artifacts...")
        self.matcher.load_artifacts()
        
        # Extract product information
        product_ids = new_products_df['product_id'].tolist()
        product_titles = new_products_df['product_title'].tolist()
        
        logger.info(f"Processing {len(product_ids)} products...")
        
        # Generate embeddings
        embeddings = self.model_loader.generate_embeddings(product_titles)
        logger.info(f"Generated embeddings with shape: {embeddings.shape}")
        
        # Find matches
        results = self.matcher.find_matches(embeddings, product_ids)
        logger.info(f"Found {len(results)} match results")
        
        # Convert to DataFrame
        results_df = self.matcher.results_to_dataframe(results)
        
        # Update index with new products
        logger.info("Updating FAISS index with new products...")
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
    
    def run_daily_pipeline(self, new_products_df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
        """
        Run the complete daily pipeline
        
        Args:
            new_products_df: DataFrame with product_id and product_title columns
            
        Returns:
            Tuple of (results_df, summary_dict)
        """
        logger.info("Starting daily pipeline execution")
        
        try:
            # Validate and clean new products
            cleaned_df = self.load_new_products(new_products_df)
            
            # Process products and find matches
            results_df = self.process_new_products(cleaned_df)
            
            # Generate summary
            summary = self.generate_summary_report(results_df)
            
            logger.info("Daily pipeline completed successfully")
            return results_df, summary
            
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
        # Load CSV
        new_products_df = pd.read_csv(new_products_csv)
        
        pipeline = DailyPipeline()
        results_df, summary = pipeline.run_daily_pipeline(new_products_df)
        
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
        
        print(f"\nResults DataFrame shape: {results_df.shape}")
        print("Daily pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Daily pipeline execution failed: {e}")
        print(f"Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()