"""Configuration file for product matching pipeline"""

from pathlib import Path

# Resolve project directories relative to this config file so paths work inside containers.
BASE_DIR = Path(__file__).resolve().parents[1]
MODEL_DIR = BASE_DIR / "models"
INDEX_DIR = BASE_DIR / "indices"
LOGS_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"

for directory in [MODEL_DIR, INDEX_DIR, LOGS_DIR, DATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Model Configuration
SBERT_MODEL_NAME = "all-MiniLM-L6-v2"
SIAMESE_MODEL_PATH = str(MODEL_DIR / "siamese_matcher.pth")
EMBEDDING_DIM = 128
SBERT_DIM = 384

# FAISS Configuration
FAISS_INDEX_PATH = str(INDEX_DIR / "product_vectors.index")
PRODUCT_ID_MAP_PATH = str(INDEX_DIR / "product_id_map.pkl")

# Matching Configuration
SIMILARITY_THRESHOLD = 0.7  # Distance threshold for matches
TOP_K_MATCHES = 5  # Number of top matches to consider
CONFIDENCE_THRESHOLD = 0.6  # Minimum confidence score for valid matches

# Database/File Paths
PRODUCTS_CSV_PATH = str(DATA_DIR / "products.csv")
RESULTS_CSV_PATH = str(BASE_DIR / "matching_results.csv")

# Batch Processing
BATCH_SIZE = 32
MAX_PRODUCTS_PER_BATCH = 1000

# FAISS Settings
FAISS_METRIC = "L2"  # L2 distance for Euclidean
FAISS_NLIST = 100  # Number of clusters for IVF index (if using large datasets)

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"