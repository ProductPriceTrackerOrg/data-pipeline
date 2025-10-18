"""
Model loading utilities for the product matching pipeline
"""
import torch
import torch.nn as nn
from sentence_transformers import SentenceTransformer
import pickle
import faiss
import numpy as np
from typing import Optional, List, Tuple
import logging
from .config import *

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class SiameseNetwork(nn.Module):
    """Siamese Network architecture matching the training script"""
    def __init__(self, input_dim=384, embedding_dim=128):
        super(SiameseNetwork, self).__init__()
        self.projection_head = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.ReLU(),
            nn.Linear(256, embedding_dim)
        )
        
    def forward_one(self, embedding):
        return self.projection_head(embedding)
    
    def forward(self, embedding1, embedding2):
        output1 = self.forward_one(embedding1)
        output2 = self.forward_one(embedding2)
        return output1, output2

class ModelLoader:
    """Handles loading and managing all models and indices"""
    
    def __init__(self):
        self.sbert_model = None
        self.siamese_model = None
        self.faiss_index = None
        self.product_id_map = None
        
    def load_sbert_model(self) -> SentenceTransformer:
        """Load the pre-trained SentenceTransformer model"""
        if self.sbert_model is None:
            logger.info(f"Loading SBERT model: {SBERT_MODEL_NAME}")
            self.sbert_model = SentenceTransformer(SBERT_MODEL_NAME)
        return self.sbert_model
    
    def load_siamese_model(self) -> SiameseNetwork:
        """Load the trained Siamese Network"""
        if self.siamese_model is None:
            logger.info(f"Loading Siamese model from: {SIAMESE_MODEL_PATH}")
            
            # Load model state
            model_state = torch.load(SIAMESE_MODEL_PATH, map_location='cpu')
            
            # Create model instance
            self.siamese_model = SiameseNetwork(
                input_dim=model_state.get('input_dim', SBERT_DIM),
                embedding_dim=model_state.get('embedding_dim', EMBEDDING_DIM)
            )
            
            # Load weights
            self.siamese_model.projection_head.load_state_dict(
                model_state['projection_head_state_dict']
            )
            self.siamese_model.eval()
            
        return self.siamese_model
    
    def load_faiss_index(self) -> Optional[faiss.Index]:
        """Load the FAISS index if it exists"""
        if self.faiss_index is None:
            try:
                logger.info(f"Loading FAISS index from: {FAISS_INDEX_PATH}")
                self.faiss_index = faiss.read_index(FAISS_INDEX_PATH)
                logger.info(f"Loaded index with {self.faiss_index.ntotal} vectors")
            except FileNotFoundError:
                logger.warning("FAISS index file not found")
                return None
        return self.faiss_index
    
    def load_product_id_map(self) -> Optional[List[int]]:
        """Load the product ID mapping"""
        if self.product_id_map is None:
            try:
                logger.info(f"Loading product ID map from: {PRODUCT_ID_MAP_PATH}")
                with open(PRODUCT_ID_MAP_PATH, 'rb') as f:
                    self.product_id_map = pickle.load(f)
                logger.info(f"Loaded {len(self.product_id_map)} product IDs")
            except FileNotFoundError:
                logger.warning("Product ID map file not found")
                return None
        return self.product_id_map
    
    def generate_embeddings(self, product_titles: List[str]) -> np.ndarray:
        """Generate final embeddings using SBERT + Siamese projection"""
        sbert_model = self.load_sbert_model()
        siamese_model = self.load_siamese_model()
        
        # Generate SBERT embeddings
        logger.info(f"Generating SBERT embeddings for {len(product_titles)} products")
        sbert_embeddings = sbert_model.encode(
            product_titles,
            convert_to_tensor=True,
            show_progress_bar=True,
            batch_size=BATCH_SIZE
        )
        
        # Generate final embeddings through projection head
        logger.info("Generating final embeddings through projection head")
        final_embeddings = []
        
        with torch.no_grad():
            for i in range(0, len(sbert_embeddings), BATCH_SIZE):
                batch = sbert_embeddings[i:i + BATCH_SIZE]
                projected = siamese_model.forward_one(batch)
                final_embeddings.append(projected.cpu().numpy())
        
        final_embeddings = np.vstack(final_embeddings)
        logger.info(f"Generated {final_embeddings.shape[0]} final embeddings of dimension {final_embeddings.shape[1]}")
        
        return final_embeddings
    
    def save_faiss_index(self, index: faiss.Index):
        """Save FAISS index to disk"""
        logger.info(f"Saving FAISS index to: {FAISS_INDEX_PATH}")
        faiss.write_index(index, FAISS_INDEX_PATH)
        self.faiss_index = index
    
    def save_product_id_map(self, product_ids: List[int]):
        """Save product ID map to disk"""
        logger.info(f"Saving product ID map to: {PRODUCT_ID_MAP_PATH}")
        with open(PRODUCT_ID_MAP_PATH, 'wb') as f:
            pickle.dump(product_ids, f)
        self.product_id_map = product_ids

# Global model loader instance
model_loader = ModelLoader()