"""Configuration for the LSTM price forecasting pipeline."""

import os
import torch

# Resolve paths relative to this module so the pipeline works from any CWD
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# BigQuery Configuration
PROJECT_ID = "price-pulse-470211"
DATASET_ID = "warehouse"
CREDENTIALS_PATH = "gcp-credentials.json"

# BigQuery Table Names
TABLE_FACT_PRODUCT_PRICE = "FactProductPrice"
TABLE_DIM_VARIANT = "DimVariant"
TABLE_DIM_SHOP_PRODUCT = "DimShopProduct"
TABLE_FACT_PRICE_FORECAST = "FactPriceForecast"
TABLE_DIM_MODEL = "DimModel"

# Model Configuration
SEQUENCE_LENGTH = 30  # Days of historical data to use for prediction
FORECAST_LENGTH = 7  # Days to forecast ahead
HIDDEN_SIZE = 128  # LSTM hidden units
NUM_LAYERS = 2  # Number of LSTM layers
BATCH_SIZE = 64  # Training batch size
LEARNING_RATE = 0.001  # Learning rate for fine-tuning
FINE_TUNE_EPOCHS =20  # Number of epochs for fine-tuning

# File Paths (ensure they resolve relative to the priceforecasting package)
LSTM_MODEL_PATH = os.path.join(BASE_DIR, "models", "lstm_forecaster.pth")
SCALERS_PATH = os.path.join(BASE_DIR, "models", "product_scalers.pkl")
DATA_CSV_PATH = os.path.join(BASE_DIR, "bigquery_prices.csv")

# Device Configuration
DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# Model ID for predictions
MODEL_ID = 100

# Data Processing Configuration
MIN_DATA_POINTS = (
    SEQUENCE_LENGTH + FORECAST_LENGTH
)  # Minimum data points needed per product
TRAIN_VALIDATION_SPLIT = 0.9  # 90% for training, 10% for validation

# Logging Configuration (write logs next to this module)
LOG_FILE = os.path.join(BASE_DIR, "production_pipeline.log")
LOG_LEVEL = "INFO"

# SQL Query Template
QUERY_GET_PRICES = """
SELECT 
    v.variant_id,
    v.variant_title,
    sp.product_title_native,
    dd.full_date as date,
    fpp.current_price as price,
    sp.shop_product_id
FROM `{project}.{dataset}.{fact_price}` fpp
JOIN `{project}.{dataset}.{dim_variant}` v ON fpp.variant_id = v.variant_id
JOIN `{project}.{dataset}.{dim_shop_product}` sp ON v.shop_product_id = sp.shop_product_id
JOIN `{project}.{dataset}.DimDate` dd ON fpp.date_id = dd.date_id
WHERE fpp.is_available = TRUE
ORDER BY v.variant_id, dd.full_date
"""
