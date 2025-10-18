"""
Backfill script to find matches in existing dataset
This runs once after bootstrap to find initial matches
"""
import pandas as pd
import numpy as np
import logging
from typing import List
from .product_matcher import ProductMatcher
from .model_loader import model_loader
from .config import *

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class BackfillMatcher:
    """Finds matches within existing dataset using the created index"""
    
    def __init__(self):
        self.matcher = ProductMatcher()
        self.model_loader = model_loader
    
    
    def run_backfill(self, df, batch_size: int = None) -> pd.DataFrame:
        """Run backfill matching process"""
        batch_size = batch_size or MAX_PRODUCTS_PER_BATCH
        
        logger.info("Starting backfill matching process...")
        
        # Load artifacts
        self.matcher.load_artifacts()
        
        
        all_results = []
        
        # Process in batches to manage memory
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}: products {i+1} to {min(i+batch_size, len(df))}")
            
            # Extract batch information
            batch_product_ids = batch_df['product_id'].tolist()
            batch_product_titles = batch_df['product_title'].tolist()
            
            # Generate embeddings for batch
            batch_embeddings = self.model_loader.generate_embeddings(batch_product_titles)
            
            # Find matches
            batch_results = self.matcher.find_matches(batch_embeddings, batch_product_ids)
            all_results.extend(batch_results)
            
            logger.info(f"Batch completed: found {len(batch_results)} matches")
        
        # Convert results to DataFrame
        results_df = self.matcher.results_to_dataframe(all_results)
        
        # Add products that didn't match to their own groups
        all_product_ids = set(df['product_id'].tolist())
        matched_product_ids = set(results_df['shop_product_id'].tolist())
        unmatched_product_ids = all_product_ids - matched_product_ids
        
        if unmatched_product_ids:
            logger.info(f"Creating individual groups for {len(unmatched_product_ids)} unmatched products")
            
            unmatched_results = []
            for product_id in unmatched_product_ids:
                new_group_id = self.matcher.create_new_match_group()
                unmatched_results.append({
                    'match_group_id': new_group_id,
                    'shop_product_id': product_id,
                    'confidence_score': 1.0
                })
            
            unmatched_df = pd.DataFrame(unmatched_results)
            unmatched_df['match_group_id'] = unmatched_df['match_group_id'].astype('Int64')
            unmatched_df['shop_product_id'] = unmatched_df['shop_product_id'].astype('Int64')
            unmatched_df['confidence_score'] = unmatched_df['confidence_score'].astype('float64')
            
            results_df = pd.concat([results_df, unmatched_df], ignore_index=True)
        
        logger.info(f"Backfill completed: {len(results_df)} total results")
        return results_df
    
    def save_results(self, results_df: pd.DataFrame, output_path: str = None):
        """Save backfill results to CSV"""
        output_path = output_path or RESULTS_CSV_PATH
        
        logger.info(f"Saving backfill results to: {output_path}")
        results_df.to_csv(output_path, index=False)
        
        # Print summary statistics
        total_products = len(results_df)
        unique_groups = results_df['match_group_id'].nunique()
        avg_confidence = results_df['confidence_score'].mean()
        
        print(f"\nBackfill Results Summary:")
        print(f"   Total Products: {total_products}")
        print(f"   Unique Match Groups: {unique_groups}")
        print(f"   Average Confidence: {avg_confidence:.4f}")
        print(f"   Results saved to: {output_path}")

