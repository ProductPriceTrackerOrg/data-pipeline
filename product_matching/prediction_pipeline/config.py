"""
Configuration file for product matching pipeline
"""
import os

# Model Configuration
SBERT_MODEL_NAME = 'all-MiniLM-L6-v2'
SIAMESE_MODEL_PATH = 'product_matching/models/siamese_matcher.pth'
EMBEDDING_DIM = 128
SBERT_DIM = 384

# FAISS Configuration
FAISS_INDEX_PATH = 'product_matching/indices/product_vectors.index'
PRODUCT_ID_MAP_PATH = 'product_matching/indices/product_id_map.pkl'

# Matching Configuration
SIMILARITY_THRESHOLD = 0.7  # Distance threshold for matches
TOP_K_MATCHES = 5  # Number of top matches to consider
CONFIDENCE_THRESHOLD = 0.6  # Minimum confidence score for valid matches

# # Database/File Paths
PRODUCTS_CSV_PATH = 'products.csv'
RESULTS_CSV_PATH = 'matching_results.csv'

# Batch Processing
BATCH_SIZE = 32
MAX_PRODUCTS_PER_BATCH = 1000

# FAISS Settings
FAISS_METRIC = 'L2'  # L2 distance for Euclidean
FAISS_NLIST = 100  # Number of clusters for IVF index (if using large datasets)

# Logging Configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# File Directories
MODEL_DIR = 'models'
INDEX_DIR = 'indices'
LOGS_DIR = 'logs'
DATA_DIR = 'data'

# Ensure directories exist
for directory in [MODEL_DIR, INDEX_DIR, LOGS_DIR, DATA_DIR]:
    os.makedirs(directory, exist_ok=True)

# Full paths
# SIAMESE_MODEL_PATH = os.path.join(MODEL_DIR, SIAMESE_MODEL_PATH)
# FAISS_INDEX_PATH = os.path.join(INDEX_DIR, FAISS_INDEX_PATH)
# PRODUCT_ID_MAP_PATH = os.path.join(INDEX_DIR, PRODUCT_ID_MAP_PATH)