"""
This file contains the main prediction logic.
It handles batch processing of product titles and generates predictions.
"""

import pandas as pd
import numpy as np
import torch
from typing import List, Dict, Tuple
import warnings
from .model_loader import ModelLoader  # Import the ModelLoader class
warnings.filterwarnings('ignore')

class CategoryPredictor:
    """
    Main predictor class that handles the prediction pipeline.
    """
    def __init__(self, model_loader: ModelLoader):
        self.model_loader = model_loader
        self.batch_size = 64  # Process in batches for efficiency
        
    def generate_embeddings(self, product_titles: List[str]) -> np.ndarray:
        """
        Generate SBERT embeddings for product titles.
        
        Args:
            product_titles: List of product title strings
            
        Returns:
            numpy array of embeddings (n_samples, 384)
        """
        try:
            print(f"Generating embeddings for {len(product_titles)} products...")
            
            # Generate embeddings in batches to manage memory
            all_embeddings = []
            
            for i in range(0, len(product_titles), self.batch_size):
                batch = product_titles[i:i + self.batch_size]
                batch_embeddings = self.model_loader.sbert_model.encode(
                    batch, 
                    show_progress_bar=False,
                    batch_size=32  # Internal SBERT batch size
                )
                all_embeddings.append(batch_embeddings)
            
            # Concatenate all batches
            embeddings = np.vstack(all_embeddings)
            print(f"Generated embeddings shape: {embeddings.shape}")
            return embeddings
            
        except Exception as e:
            print(f"Error generating embeddings: {str(e)}")
            raise
    
    def predict_categories(self, embeddings: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Predict categories from embeddings.
        
        Args:
            embeddings: SBERT embeddings array
            
        Returns:
            Tuple of (predicted_category_ids, confidence_scores)
        """
        try:
            print("Making predictions...")
            
            # Convert to PyTorch tensor
            embeddings_tensor = torch.FloatTensor(embeddings).to(self.model_loader.device)
            
            all_predictions = []
            all_confidences = []
            
            # Process in batches
            with torch.no_grad():
                for i in range(0, len(embeddings_tensor), self.batch_size):
                    batch = embeddings_tensor[i:i + self.batch_size]
                    
                    # Get model outputs
                    outputs = self.model_loader.model(batch)
                    
                    # Get predictions and confidence scores
                    probabilities = torch.softmax(outputs, dim=1)
                    confidences, predictions = torch.max(probabilities, dim=1)
                    
                    all_predictions.extend(predictions.cpu().numpy())
                    all_confidences.extend(confidences.cpu().numpy())
            
            predictions_array = np.array(all_predictions)
            confidences_array = np.array(all_confidences)
            
            print(f"Generated {len(predictions_array)} predictions")
            return predictions_array, confidences_array
            
        except Exception as e:
            print(f"Error making predictions: {str(e)}")
            raise
    
    def predict_dataframe(self, df: pd.DataFrame, 
                         title_column: str = 'product_title',
                         id_column: str = 'product_id') -> pd.DataFrame:
        """
        Main prediction method for pandas DataFrame.
        
        Args:
            df: Input DataFrame with product_id and product_title columns
            title_column: Name of the product title column
            id_column: Name of the product ID column
            
        Returns:
            DataFrame with product_id and predicted_category_id columns
        """
        try:
            print(f"Starting prediction for {len(df)} products...")
            
            # Validate input columns
            if title_column not in df.columns:
                raise ValueError(f"Column '{title_column}' not found in DataFrame")
            if id_column not in df.columns:
                raise ValueError(f"Column '{id_column}' not found in DataFrame")
            
            # Clean product titles - handle missing values
            product_titles = df[title_column].fillna("").astype(str).tolist()
            product_ids = df[id_column].tolist()
            
            # Generate embeddings
            embeddings = self.generate_embeddings(product_titles)
            
            # Make predictions
            predicted_category_ids, confidence_scores = self.predict_categories(embeddings)
            
            # Create result DataFrame
            result_df = pd.DataFrame({
                'product_id': product_ids,
                'predicted_category_id': predicted_category_ids,
                'confidence_score': confidence_scores  # Optional: include confidence
            })
            
            # Add category names (optional)
            result_df['predicted_category_name'] = result_df['predicted_category_id'].map(
                self.model_loader.category_mapping
            )
            
            print(f"Prediction complete!")
            print(f"Result shape: {result_df.shape}")
            
            return result_df
            
        except Exception as e:
            print(f"Error in prediction pipeline: {str(e)}")
            raise
    
    def get_prediction_summary(self, predictions_df: pd.DataFrame) -> Dict:
        """
        Generate summary statistics of predictions.
        """
        summary = {
            'total_products': len(predictions_df),
            'unique_categories_predicted': predictions_df['predicted_category_id'].nunique(),
            'average_confidence': predictions_df['confidence_score'].mean(),
            'min_confidence': predictions_df['confidence_score'].min(),
            'max_confidence': predictions_df['confidence_score'].max(),
            'category_distribution': predictions_df['predicted_category_name'].value_counts().to_dict()
        }
        return summary