"""
Data Processing Module for LSTM Price Forecasting
"""

import numpy as np
import pandas as pd
import pickle
import logging
from sklearn.preprocessing import MinMaxScaler
import config

logger = logging.getLogger(__name__)


class DataProcessor:
    """Handles data processing for the LSTM price forecasting pipeline"""

    def __init__(self):
        self.scalers = {}

    def save_to_csv(self, df, filename):
        """
        Save DataFrame to CSV file

        Args:
            df (pd.DataFrame): Data to save
            filename (str): CSV filename

        Returns:
            bool: Success status
        """
        try:
            df.to_csv(filename, index=False)
            logger.info(f"SUCCESS: Saved data to {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save CSV: {e}")
            return False

    def create_sequences_with_per_product_scaling(self, data):
        """
        Create sliding window sequences with per-product normalization

        Args:
            data (pd.DataFrame): DataFrame with columns [product_id, date, price]

        Returns:
            tuple: (sequences, targets, product_indices) or (None, None, None) on failure
        """
        try:
            logger.info("Creating sequences with per-product scaling...")

            all_sequences = []
            all_targets = []
            scalers = {}
            product_indices = []

            unique_products = data["product_id"].unique()
            logger.info(f"Processing {len(unique_products)} unique products...")

            for idx, product_id in enumerate(unique_products):
                if (idx + 1) % 100 == 0:
                    logger.info(
                        f"  Processed {idx + 1}/{len(unique_products)} products..."
                    )

                product_data = data[data["product_id"] == product_id][
                    "price"
                ].values.reshape(-1, 1)

                # Skip products with insufficient data
                if len(product_data) < config.MIN_DATA_POINTS:
                    continue

                # Create and fit scaler for THIS product only
                scaler = MinMaxScaler()
                scaled_data = scaler.fit_transform(product_data).flatten()
                scalers[product_id] = scaler

                # Create sliding windows for this product
                for i in range(
                    len(scaled_data)
                    - config.SEQUENCE_LENGTH
                    - config.FORECAST_LENGTH
                    + 1
                ):
                    seq = scaled_data[i : i + config.SEQUENCE_LENGTH]
                    target = scaled_data[
                        i
                        + config.SEQUENCE_LENGTH : i
                        + config.SEQUENCE_LENGTH
                        + config.FORECAST_LENGTH
                    ]
                    all_sequences.append(seq)
                    all_targets.append(target)
                    product_indices.append(product_id)

            self.scalers = scalers

            logger.info(
                f"SUCCESS: Created {len(all_sequences)} sequences from {len(scalers)} products"
            )

            return np.array(all_sequences), np.array(all_targets), product_indices

        except Exception as e:
            logger.error(f"Failed to create sequences: {e}")
            return None, None, None

    def split_train_validation(self, X, y, split_ratio=None):
        """
        Split data into training and validation sets

        Args:
            X (np.array): Input sequences
            y (np.array): Target sequences
            split_ratio (float): Training split ratio

        Returns:
            tuple: (X_train, X_val, y_train, y_val)
        """
        if split_ratio is None:
            split_ratio = config.TRAIN_VALIDATION_SPLIT

        split_idx = int(split_ratio * len(X))
        X_train = X[:split_idx]
        X_val = X[split_idx:]
        y_train = y[:split_idx]
        y_val = y[split_idx:]

        logger.info(f"Training sequences: {len(X_train)}")
        logger.info(f"Validation sequences: {len(X_val)}")

        return X_train, X_val, y_train, y_val

    def save_scalers(self, filepath=None):
        """
        Save scalers to pickle file

        Args:
            filepath (str): Path to save scalers
        """
        if filepath is None:
            filepath = config.SCALERS_PATH

        try:
            with open(filepath, "wb") as f:
                pickle.dump(self.scalers, f)
            logger.info(f"SUCCESS: Saved scalers to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save scalers: {e}")

    def load_scalers(self, filepath=None):
        """
        Load scalers from pickle file

        Args:
            filepath (str): Path to load scalers from

        Returns:
            bool: Success status
        """
        if filepath is None:
            filepath = config.SCALERS_PATH

        try:
            with open(filepath, "rb") as f:
                self.scalers = pickle.load(f)
            logger.info(f"SUCCESS: Loaded scalers from {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to load scalers: {e}")
            return False

    def cleanup_file(self, filename):
        """
        Delete a file after processing

        Args:
            filename (str): File to delete
        """
        import os

        try:
            if os.path.exists(filename):
                os.remove(filename)
                logger.info(f"SUCCESS: Deleted file: {filename}")
            else:
                logger.warning(f"File not found: {filename}")
        except Exception as e:
            logger.error(f"Failed to delete file: {e}")
