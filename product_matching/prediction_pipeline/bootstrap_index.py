"""
Bootstrap script to create initial FAISS index and ID mapping
This is a one-time setup script
"""
import pandas as pd
import numpy as np
import faiss
import logging
from typing import List, Tuple
from .model_loader import model_loader
from .config import *

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class IndexBootstrapper:
    """Creates initial FAISS index and ID mapping from existing products"""
    
    def __init__(self):
        self.model_loader = model_loader
    
    
    def create_faiss_index(self, embeddings: np.ndarray) -> faiss.Index:
        """Create FAISS index from embeddings"""
        dimension = embeddings.shape[1]
        logger.info(f"Creating FAISS index for {embeddings.shape[0]} vectors of dimension {dimension}")
        
        # For small datasets, use simple flat index
        # For larger datasets (>10k), consider IVF index
        if embeddings.shape[0] < 10000:
            index = faiss.IndexFlatL2(dimension)
        else:
            # Use IVF index for larger datasets
            quantizer = faiss.IndexFlatL2(dimension)
            nlist = min(FAISS_NLIST, embeddings.shape[0] // 39)  # Rule of thumb
            index = faiss.IndexIVFFlat(quantizer, dimension, nlist)
            
            # Train the index
            logger.info("Training IVF index...")
            index.train(embeddings.astype(np.float32))
        
        # Add vectors to index
        logger.info("Adding vectors to index...")
        index.add(embeddings.astype(np.float32))
        
        logger.info(f"Index created with {index.ntotal} vectors")
        return index

    def bootstrap(self, df: pd.DataFrame) -> Tuple[faiss.Index, List[int]]:
        """Main bootstrap process"""
        logger.info("Starting bootstrap process...")
        
        # Extract product information
        product_ids = df['product_id'].tolist()
        product_titles = df['product_title'].tolist()
        
        # Generate embeddings
        embeddings = self.model_loader.generate_embeddings(product_titles)
        
        # Create FAISS index
        index = self.create_faiss_index(embeddings)
        
        # Save index and ID mapping
        self.model_loader.save_faiss_index(index)
        self.model_loader.save_product_id_map(product_ids)
        
        logger.info("Bootstrap process completed successfully")
        return index, product_ids

def main():
    """Main function to run bootstrap process"""
    try:
        bootstrapper = IndexBootstrapper()
        index, product_ids = bootstrapper.bootstrap()
        
        print(f"Successfully created index with {len(product_ids)} products")
        print(f"Index saved to: {FAISS_INDEX_PATH}")
        print(f"ID map saved to: {PRODUCT_ID_MAP_PATH}")
        
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        raise

if __name__ == "__main__":
    main()