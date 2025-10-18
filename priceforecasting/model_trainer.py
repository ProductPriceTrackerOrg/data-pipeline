"""
Model Training and Prediction Module for LSTM Price Forecasting
"""

import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
import logging
from datetime import datetime, timedelta
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

from models.lstm_model import LSTMSeq2Seq, TimeSeriesDataset
import config

logger = logging.getLogger(__name__)


class ModelTrainer:
    """Handles model training, fine-tuning, and prediction generation"""

    def __init__(self):
        self.model = None
        self.device = config.DEVICE

    def load_pretrained_model(self, model_path=None):
        """
        Load pre-trained LSTM model

        Args:
            model_path (str): Path to the model file

        Returns:
            bool: Success status
        """
        if model_path is None:
            model_path = config.LSTM_MODEL_PATH

        try:
            if not os.path.exists(model_path):
                logger.error(f"Model file not found: {model_path}")
                return False

            self.model = LSTMSeq2Seq(
                input_size=1,
                hidden_size=config.HIDDEN_SIZE,
                num_layers=config.NUM_LAYERS,
                output_size=1,
                forecast_length=config.FORECAST_LENGTH,
            ).to(self.device)

            self.model.load_state_dict(torch.load(model_path, map_location=self.device))
            logger.info(f"SUCCESS: Loaded pre-trained LSTM model from {model_path}")
            logger.info(
                f"Model parameters: {sum(p.numel() for p in self.model.parameters()):,}"
            )
            logger.info(f"Device: {self.device}")
            return True

        except Exception as e:
            logger.error(f"Failed to load pre-trained model: {e}")
            return False

    def calculate_metrics(self, loader, criterion):
        """
        Calculate comprehensive metrics for a dataset

        Args:
            loader: DataLoader for the dataset
            criterion: Loss function

        Returns:
            dict: Dictionary containing loss, MAE, MAPE, and accuracy metrics
        """
        self.model.eval()
        total_loss = 0.0
        all_predictions = []
        all_targets = []

        with torch.no_grad():
            for batch_X, batch_y in loader:
                batch_X = batch_X.to(self.device)
                batch_y = batch_y.to(self.device)

                outputs = self.model(batch_X)
                loss = criterion(outputs, batch_y)
                total_loss += loss.item()

                # Collect predictions and targets for metric calculation
                all_predictions.extend(outputs.cpu().numpy().flatten())
                all_targets.extend(batch_y.cpu().numpy().flatten())

        # Convert to numpy arrays
        predictions = np.array(all_predictions)
        targets = np.array(all_targets)

        # Calculate metrics
        avg_loss = total_loss / len(loader)
        mae = mean_absolute_error(targets, predictions)

        # Convert MAE to percentage relative to the target mean
        mae_percentage = (mae / (np.mean(np.abs(targets)) + 1e-8)) * 100

        # Calculate MAPE (avoid division by zero and handle scaled data properly)
        # Since we're working with scaled data (0-1 range), we need to be careful with MAPE
        epsilon = 1e-8
        # Only calculate MAPE for non-zero targets to avoid infinity
        non_zero_mask = np.abs(targets) > epsilon
        if np.sum(non_zero_mask) > 0:
            mape = (
                np.mean(
                    np.abs(
                        (targets[non_zero_mask] - predictions[non_zero_mask])
                        / (targets[non_zero_mask] + epsilon)
                    )
                )
                * 100
            )
        else:
            mape = 0.0

        # Calculate accuracy (within tolerance for scaled data)
        # For scaled data (0-1), use absolute difference tolerance instead of percentage
        tolerance = 0.05  # 5% tolerance for better accuracy measure
        accurate_predictions = np.abs(targets - predictions) <= tolerance
        accuracy = np.mean(accurate_predictions) * 100

        return {
            "loss": avg_loss,
            "mae": mae,
            "mae_percentage": mae_percentage,
            "mape": mape,
            "accuracy": accuracy,
        }

    def fine_tune_model(self, X_train, y_train, X_val=None, y_val=None):
        """
        Fine-tune the LSTM model on new data

        Args:
            X_train (np.array): Training sequences
            y_train (np.array): Training targets
            X_val (np.array): Validation sequences (optional)
            y_val (np.array): Validation targets (optional)

        Returns:
            bool: Success status
        """
        try:
            logger.info("Starting model fine-tuning...")

            # Create dataset and dataloader
            train_dataset = TimeSeriesDataset(X_train, y_train)
            train_loader = DataLoader(
                train_dataset, batch_size=config.BATCH_SIZE, shuffle=True
            )

            # Validation loader if validation data provided
            val_loader = None
            if X_val is not None and y_val is not None:
                val_dataset = TimeSeriesDataset(X_val, y_val)
                val_loader = DataLoader(
                    val_dataset, batch_size=config.BATCH_SIZE, shuffle=False
                )

            # Set up training
            criterion = nn.MSELoss()
            optimizer = optim.Adam(self.model.parameters(), lr=config.LEARNING_RATE)

            print(f"\n{'='*80}")
            print("FINE-TUNING PROGRESS")
            print(f"{'='*80}")
            print(
                f"{'Epoch':<6} {'Train Loss':<12} {'Train MAE%':<11} {'Train MAPE%':<12} {'Train Acc%':<11} {'Test Loss':<11} {'Test MAE%':<10} {'Test MAPE%':<11} {'Test Acc%':<10}"
            )
            print(f"{'-'*80}")

            # Store final metrics for summary
            final_train_metrics = None
            final_test_metrics = None

            for epoch in range(config.FINE_TUNE_EPOCHS):
                # Training phase
                self.model.train()
                total_train_loss = 0.0
                num_train_batches = 0

                for batch_X, batch_y in train_loader:
                    batch_X = batch_X.to(self.device)
                    batch_y = batch_y.to(self.device)

                    optimizer.zero_grad()
                    outputs = self.model(batch_X)
                    loss = criterion(outputs, batch_y)
                    loss.backward()
                    optimizer.step()

                    total_train_loss += loss.item()
                    num_train_batches += 1

                # Calculate training metrics
                train_metrics = self.calculate_metrics(train_loader, criterion)
                final_train_metrics = train_metrics  # Store for final summary

                # Calculate validation metrics if validation data provided
                if val_loader is not None:
                    val_metrics = self.calculate_metrics(val_loader, criterion)
                    final_test_metrics = val_metrics  # Store for final summary

                    # Print metrics for both training and validation
                    print(
                        f"{epoch+1:<6} {train_metrics['loss']:<12.6f} {train_metrics['mae_percentage']:<11.2f} {train_metrics['mape']:<12.2f} {train_metrics['accuracy']:<11.2f} {val_metrics['loss']:<11.6f} {val_metrics['mae_percentage']:<10.2f} {val_metrics['mape']:<11.2f} {val_metrics['accuracy']:<10.2f}"
                    )
                else:
                    # Print only training metrics
                    print(
                        f"{epoch+1:<6} {train_metrics['loss']:<12.6f} {train_metrics['mae_percentage']:<11.2f} {train_metrics['mape']:<12.2f} {train_metrics['accuracy']:<11.2f} {'N/A':<11} {'N/A':<10} {'N/A':<11} {'N/A':<10}"
                    )

            print(f"{'-'*80}")

            # Print final summary
            print("\nFINAL RESULTS SUMMARY:")
            print(f"{'='*50}")
            if final_train_metrics:
                print(f"FINAL TRAINING METRICS:")
                print(f"  • Accuracy: {final_train_metrics['accuracy']:.2f}%")
                print(f"  • MAE: {final_train_metrics['mae_percentage']:.2f}%")
                print(f"  • MAPE: {final_train_metrics['mape']:.2f}%")
                print(f"  • Loss: {final_train_metrics['loss']:.6f}")

            if final_test_metrics:
                print(f"\nFINAL TEST METRICS:")
                print(f"  • Accuracy: {final_test_metrics['accuracy']:.2f}%")
                print(f"  • MAE: {final_test_metrics['mae_percentage']:.2f}%")
                print(f"  • MAPE: {final_test_metrics['mape']:.2f}%")
                print(f"  • Loss: {final_test_metrics['loss']:.6f}")

            print(f"{'='*50}")
            print("Fine-tuning completed!")
            print(f"{'='*80}\n")

            # Save fine-tuned model
            fine_tuned_path = config.LSTM_MODEL_PATH.replace(".pth", "_finetuned.pth")
            torch.save(self.model.state_dict(), fine_tuned_path)

            logger.info(
                f"SUCCESS: Fine-tuning completed. Model saved to {fine_tuned_path}"
            )
            return True

        except Exception as e:
            logger.error(f"Fine-tuning failed: {e}")
            return False

    def generate_predictions(self, df, scalers, model_id=None):
        """
        Generate 7-day predictions for all variants

        Args:
            df (pd.DataFrame): Historical data
            scalers (dict): Product-specific scalers
            model_id (int): Model ID for predictions

        Returns:
            list: List of prediction dictionaries
        """
        if model_id is None:
            model_id = config.MODEL_ID

        try:
            logger.info("Generating 7-day predictions...")

            if self.model is None:
                logger.error("Model not loaded. Call load_pretrained_model() first.")
                return []

            self.model.eval()
            predictions = []

            # Get unique variants for prediction
            unique_variants = df["product_id"].unique()

            with torch.no_grad():
                for variant_id in unique_variants:
                    try:
                        # Get latest prices for this variant
                        variant_data = df[df["product_id"] == variant_id].sort_values(
                            "date"
                        )

                        if len(variant_data) < config.SEQUENCE_LENGTH:
                            continue

                        # Get last sequence_length prices
                        latest_prices = (
                            variant_data["price"]
                            .values[-config.SEQUENCE_LENGTH :]
                            .astype(float)
                        )

                        # Scale using the variant's scaler
                        if variant_id in scalers:
                            scaler = scalers[variant_id]
                            scaled_prices = scaler.transform(
                                latest_prices.reshape(-1, 1)
                            ).flatten()
                        else:
                            # Use a default scaler if variant scaler not available
                            scaler = MinMaxScaler()
                            scaled_prices = scaler.fit_transform(
                                latest_prices.reshape(-1, 1)
                            ).flatten()

                        # Generate predictions
                        input_tensor = (
                            torch.FloatTensor(scaled_prices)
                            .unsqueeze(0)
                            .to(self.device)
                        )
                        pred_scaled = self.model(input_tensor).cpu().numpy().flatten()

                        # Inverse transform predictions
                        pred_prices = scaler.inverse_transform(
                            pred_scaled.reshape(-1, 1)
                        ).flatten()

                        # Calculate confidence intervals (simple approach using historical std)
                        price_std = float(variant_data["price"].std())
                        confidence_margin = 1.96 * price_std  # 95% confidence

                        # Create prediction records for 7 days
                        base_date = datetime.now().date() + timedelta(days=1)

                        for i, pred_price in enumerate(pred_prices):
                            forecast_date = base_date + timedelta(days=i)

                            predictions.append(
                                {
                                    "variant_id": int(variant_id),
                                    "model_id": model_id,
                                    "forecast_date": forecast_date.strftime(
                                        "%Y-%m-%d"
                                    ),  # Convert to string format
                                    "predicted_price": round(float(pred_price), 2),
                                    "confidence_upper": round(
                                        float(pred_price + confidence_margin), 2
                                    ),
                                    "confidence_lower": round(
                                        float(max(0, pred_price - confidence_margin)), 2
                                    ),
                                    "created_at": datetime.now(),
                                }
                            )

                    except Exception as e:
                        logger.warning(
                            f"Failed to predict for variant {variant_id}: {e}"
                        )
                        continue

            logger.info(
                f"SUCCESS: Generated {len(predictions)} predictions for {len(unique_variants)} variants"
            )
            return predictions

        except Exception as e:
            logger.error(f"Prediction generation failed: {e}")
            return []

    def save_model(self, filepath, suffix=""):
        """
        Save current model state

        Args:
            filepath (str): Base filepath for saving
            suffix (str): Suffix to add to filename
        """
        try:
            if suffix:
                save_path = filepath.replace(".pth", f"_{suffix}.pth")
            else:
                save_path = filepath

            torch.save(self.model.state_dict(), save_path)
            logger.info(f"SUCCESS: Model saved to {save_path}")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")
