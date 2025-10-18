"""
Utility functions for inference and batch processing
"""
import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Tuple, Optional
from .product_matcher import ProductMatcher, MatchResult
from .model_loader import model_loader
from .config import *

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class InferenceUtils:
    """Utility functions for product matching inference"""
    
    def __init__(self):
        self.matcher = ProductMatcher()
        self.model_loader = model_loader
        self._artifacts_loaded = False
    
    def load_artifacts_once(self):
        """Load artifacts only once for efficiency"""
        if not self._artifacts_loaded:
            self.matcher.load_artifacts()
            self._artifacts_loaded = True
    
    def match_single_product(self, product_id: int, product_title: str) -> MatchResult:
        """Match a single product against the index"""
        self.load_artifacts_once()
        
        # Generate embedding
        embedding = self.model_loader.generate_embeddings([product_title])
        
        # Find matches
        results = self.matcher.find_matches(embedding, [product_id])
        
        return results[0] if results else None
    
    def match_product_batch(self, products_df: pd.DataFrame) -> pd.DataFrame:
        """Match a batch of products"""
        self.load_artifacts_once()
        
        if len(products_df) == 0:
            return pd.DataFrame(columns=['match_group_id', 'shop_product_id', 'confidence_score'])
        
        # Extract information
        product_ids = products_df['product_id'].tolist()
        product_titles = products_df['product_title'].tolist()
        
        # Generate embeddings
        embeddings = self.model_loader.generate_embeddings(product_titles)
        
        # Find matches
        results = self.matcher.find_matches(embeddings, product_ids)
        
        # Convert to DataFrame
        return self.matcher.results_to_dataframe(results)
    
    def get_match_group_members(self, match_group_id: int, 
                               results_csv: str = None) -> List[int]:
        """Get all product IDs in a specific match group"""
        results_csv = results_csv or RESULTS_CSV_PATH
        
        try:
            results_df = pd.read_csv(results_csv)
            members = results_df[
                results_df['match_group_id'] == match_group_id
            ]['shop_product_id'].tolist()
            return members
        except FileNotFoundError:
            logger.warning(f"Results file not found: {results_csv}")
            return []
    
    def find_similar_products(self, product_title: str, 
                            top_k: int = 5) -> List[Dict]:
        """Find similar products for a given title without updating index"""
        self.load_artifacts_once()
        
        # Generate embedding
        query_embedding = self.model_loader.generate_embeddings([product_title])
        
        # Search index
        distances, indices = self.matcher.faiss_index.search(
            query_embedding.astype(np.float32), 
            top_k
        )
        
        results = []
        for distance, index in zip(distances[0], indices[0]):
            if index == -1:
                continue
            
            product_id = self.matcher.product_id_map[index]
            confidence = self.matcher.distance_to_confidence(distance)
            
            results.append({
                'product_id': product_id,
                'distance': float(distance),
                'confidence_score': confidence
            })
        
        return results
    
    def validate_index_consistency(self) -> Dict[str, any]:
        """Validate consistency between FAISS index and ID mapping"""
        self.load_artifacts_once()
        
        index_size = self.matcher.faiss_index.ntotal
        map_size = len(self.matcher.product_id_map)
        
        validation_result = {
            'is_consistent': index_size == map_size,
            'index_size': index_size,
            'map_size': map_size,
            'size_difference': abs(index_size - map_size)
        }
        
        if validation_result['is_consistent']:
            logger.info("Index and ID mapping are consistent")
        else:
            logger.warning(f"Inconsistency detected: Index={index_size}, Map={map_size}")
        
        return validation_result
    
    def get_index_statistics(self) -> Dict[str, any]:
        """Get statistics about the current index"""
        self.load_artifacts_once()
        
        stats = {
            'total_products': self.matcher.faiss_index.ntotal,
            'index_dimension': self.matcher.faiss_index.d,
            'index_type': type(self.matcher.faiss_index).__name__,
            'is_trained': getattr(self.matcher.faiss_index, 'is_trained', True)
        }
        
        return stats
    
    def export_embeddings(self, output_path: str, max_products: int = None):
        """Export embeddings from FAISS index to numpy file"""
        self.load_artifacts_once()
        
        total_products = self.matcher.faiss_index.ntotal
        if max_products:
            total_products = min(total_products, max_products)
        
        logger.info(f"Exporting {total_products} embeddings to {output_path}")
        
        # Reconstruct embeddings from index
        embeddings = self.matcher.faiss_index.reconstruct_n(0, total_products)
        product_ids = self.matcher.product_id_map[:total_products]
        
        # Save as numpy arrays
        np.savez(
            output_path,
            embeddings=embeddings,
            product_ids=np.array(product_ids)
        )
        
        logger.info(f"Embeddings exported successfully")
    
    def create_similarity_matrix(self, product_ids: List[int]) -> np.ndarray:
        """Create similarity matrix for given product IDs"""
        self.load_artifacts_once()
        
        # Find indices for given product IDs
        indices = []
        for pid in product_ids:
            try:
                idx = self.matcher.product_id_map.index(pid)
                indices.append(idx)
            except ValueError:
                logger.warning(f"Product ID {pid} not found in index")
        
        if not indices:
            return np.array([])
        
        # Get embeddings
        embeddings = np.array([
            self.matcher.faiss_index.reconstruct(idx) for idx in indices
        ])
        
        # Calculate pairwise distances
        from sklearn.metrics.pairwise import euclidean_distances
        distance_matrix = euclidean_distances(embeddings)
        
        # Convert to similarity (inverse of distance)
        similarity_matrix = 1.0 / (1.0 + distance_matrix)
        
        return similarity_matrix

def create_sample_inference_script():
    """Create a sample script showing how to use inference utilities"""
    sample_script = '''
# Sample inference script
from inference_utils import InferenceUtils
import pandas as pd

# Initialize inference utils
inference = InferenceUtils()

# Example 1: Match a single product
result = inference.match_single_product(
    product_id=12345,
    product_title="iPhone 13 Pro Max 256GB Blue"
)
print(f"Match Group: {result.match_group_id}, Confidence: {result.confidence_score}")

# Example 2: Match a batch of products
new_products = pd.DataFrame({
    'product_id': [1001, 1002, 1003],
    'product_title': [
        "Samsung Galaxy S21",
        "Nike Air Force 1 White",
        "Dell Laptop 15 inch"
    ]
})

results_df = inference.match_product_batch(new_products)
print(results_df)

# Example 3: Find similar products
similar = inference.find_similar_products("MacBook Pro 13 inch", top_k=3)
for item in similar:
    print(f"Product {item['product_id']}: {item['confidence_score']:.4f}")
'''
    
    with open('sample_inference.py', 'w') as f:
        f.write(sample_script)
    
    print("Sample inference script created: sample_inference.py")

if __name__ == "__main__":
    # Create sample inference script
    create_sample_inference_script()
    
    # Run some basic validation
    inference = InferenceUtils()
    
    try:
        # Validate index consistency
        validation = inference.validate_index_consistency()
        print(f"Index Validation: {validation}")
        
        # Get index statistics
        stats = inference.get_index_statistics()
        print(f"Index Statistics: {stats}")
        
    except Exception as e:
        print(f"Could not run validation (likely no index exists yet): {e}")