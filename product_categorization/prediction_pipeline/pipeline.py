"""
Main script to run the prediction pipeline.
This is what you'll integrate into your website workflow.
"""

import pandas as pd
import sys
import time
from datetime import datetime

# Import our custom modules
from .model_loader import ModelLoader
from .predictor import CategoryPredictor

def run_prediction_pipeline(input_df: pd.DataFrame, 
                          model_path: str = 'ann_category_classifier.pth') -> pd.DataFrame:
    """
    Main function to run the complete prediction pipeline.
    
    Args:
        input_df: DataFrame with 'product_id' and 'product_title' columns
        model_path: Path to the trained model file
        
    Returns:
        DataFrame with 'product_id' and 'predicted_category_id' columns
    """
    
    print("="*60)
    print("PRODUCT CATEGORY PREDICTION PIPELINE")
    print("="*60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Input data shape: {input_df.shape}")
    
    try:
        # Step 1: Initialize model loader
        print("\n1. Initializing models...")
        model_loader = ModelLoader(model_path=model_path)
        model_loader.initialize_all()
        
        # Step 2: Initialize predictor
        print("\n2. Setting up predictor...")
        predictor = CategoryPredictor(model_loader)
        
        # Step 3: Run predictions
        print("\n3. Running predictions...")
        start_time = time.time()
        
        predictions_df = predictor.predict_dataframe(input_df)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Step 4: Generate summary
        print("\n4️. Generating summary...")
        summary = predictor.get_prediction_summary(predictions_df)
        
        print(f"\nPREDICTION SUMMARY:")
        print(f"Total products processed: {summary['total_products']:,}")
        print(f"Processing time: {processing_time:.2f} seconds")
        print(f"Products per second: {summary['total_products']/processing_time:.1f}")
        print(f"Unique categories predicted: {summary['unique_categories_predicted']}")
        print(f"Average confidence: {summary['average_confidence']:.3f}")
        print(f"Confidence range: {summary['min_confidence']:.3f} - {summary['max_confidence']:.3f}")
        
        print(f"\nTOP 5 PREDICTED CATEGORIES:")
        for category, count in list(summary['category_distribution'].items())[:5]:
            print(f"  {category}: {count} products")
        
        # Return only the required columns for your final result
        final_result = predictions_df[['product_id', 'predicted_category_id']].copy()
        
        print(f"\nPipeline completed successfully!")
        print(f"Final result shape: {final_result.shape}")
        
        return final_result
        
    except Exception as e:
        print(f"\nPipeline failed with error: {str(e)}")
        sys.exit(1)