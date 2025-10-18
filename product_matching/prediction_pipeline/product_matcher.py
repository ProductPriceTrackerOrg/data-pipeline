"""
Core product matching functionality using FAISS similarity search
"""
import numpy as np
import pandas as pd
import faiss
import logging
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
from .model_loader import model_loader
from .config import *

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

@dataclass
class MatchResult:
    """Data class for match results"""
    shop_product_id: int
    match_group_id: int
    confidence_score: float

class ProductMatcher:
    """Handles product matching using FAISS similarity search"""
    
    def __init__(self):
        self.model_loader = model_loader
        self.next_match_group_id = 1
        self.match_group_map = {}  # product_id -> match_group_id
        
    def load_artifacts(self):
        """Load all necessary artifacts"""
        self.faiss_index = self.model_loader.load_faiss_index()
        self.product_id_map = self.model_loader.load_product_id_map()
        
        if self.faiss_index is None or self.product_id_map is None:
            raise ValueError("FAISS index or product ID map not found. Run bootstrap first.")
        
        # Initialize next match group ID
        if self.product_id_map:
            self.next_match_group_id = len(set(self.product_id_map)) + 1
    
    def distance_to_confidence(self, distance: float) -> float:
        """Convert L2 distance to confidence score (0-1)"""
        # Sigmoid-like transformation to map distance to confidence
        # Lower distance = higher confidence
        confidence = 1.0 / (1.0 + distance)
        return min(max(confidence, 0.0), 1.0)
    
    def find_matches(self, query_embeddings: np.ndarray, 
                    query_product_ids: List[int]) -> List[MatchResult]:
        """Find matches for query embeddings using FAISS search"""
        logger.info(f"Finding matches for {len(query_embeddings)} query products")
        
        # Search FAISS index
        distances, indices = self.faiss_index.search(
            query_embeddings.astype(np.float32), 
            TOP_K_MATCHES
        )
        
        results = []
        
        for i, (query_product_id, query_distances, query_indices) in enumerate(
            zip(query_product_ids, distances, indices)
        ):
            best_match_found = False
            
            # Check each match in order of similarity
            for distance, match_index in zip(query_distances, query_indices):
                if match_index == -1:  # No match found
                    continue
                
                # Convert distance to confidence
                confidence = self.distance_to_confidence(distance)
                
                # Check if confidence meets threshold
                if confidence >= CONFIDENCE_THRESHOLD:
                    # Get the matched product ID
                    matched_product_id = self.product_id_map[match_index]
                    
                    # Skip self-matches (if query product is already in index)
                    if matched_product_id == query_product_id:
                        continue
                    
                    # Get or create match group ID
                    match_group_id = self.get_or_create_match_group(matched_product_id)
                    
                    results.append(MatchResult(
                        shop_product_id=query_product_id,
                        match_group_id=match_group_id,
                        confidence_score=confidence
                    ))
                    
                    # Add query product to the same match group
                    self.match_group_map[query_product_id] = match_group_id
                    best_match_found = True
                    break
            
            # If no match found, create new match group
            if not best_match_found:
                new_match_group_id = self.create_new_match_group()
                results.append(MatchResult(
                    shop_product_id=query_product_id,
                    match_group_id=new_match_group_id,
                    confidence_score=1.0  # Perfect confidence for new group
                ))
                self.match_group_map[query_product_id] = new_match_group_id
        
        logger.info(f"Found {len(results)} match results")
        return results
    
    def get_or_create_match_group(self, product_id: int) -> int:
        """Get existing match group or create new one"""
        if product_id in self.match_group_map:
            return self.match_group_map[product_id]
        else:
            # Create new match group
            match_group_id = self.create_new_match_group()
            self.match_group_map[product_id] = match_group_id
            return match_group_id
    
    def create_new_match_group(self) -> int:
        """Create a new match group ID"""
        match_group_id = self.next_match_group_id
        self.next_match_group_id += 1
        return match_group_id
    
    def update_index(self, new_embeddings: np.ndarray, new_product_ids: List[int]):
        """Add new embeddings and product IDs to the index"""
        logger.info(f"Adding {len(new_embeddings)} new products to index")
        
        # Add to FAISS index
        self.faiss_index.add(new_embeddings.astype(np.float32))
        
        # Update product ID mapping
        self.product_id_map.extend(new_product_ids)
        
        # Save updated artifacts
        self.model_loader.save_faiss_index(self.faiss_index)
        self.model_loader.save_product_id_map(self.product_id_map)
        
        logger.info(f"Index now contains {self.faiss_index.ntotal} products")
    
    def results_to_dataframe(self, results: List[MatchResult]) -> pd.DataFrame:
        """Convert match results to DataFrame"""
        data = []
        for result in results:
            data.append({
                'match_group_id': result.match_group_id,
                'shop_product_id': result.shop_product_id,
                'confidence_score': result.confidence_score
            })
        
        df = pd.DataFrame(data)
        
        # Ensure correct data types
        df['match_group_id'] = df['match_group_id'].astype('Int64')
        df['shop_product_id'] = df['shop_product_id'].astype('Int64')
        df['confidence_score'] = df['confidence_score'].astype('float64')
        
        return df