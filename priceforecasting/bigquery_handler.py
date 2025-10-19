"""
BigQuery Data Handler for Price Forecasting Pipeline
"""

import pandas as pd
import logging
from datetime import datetime
from google.cloud import bigquery
import config

logger = logging.getLogger(__name__)


class BigQueryHandler:
    """Handles all BigQuery operations for the price forecasting pipeline"""

    def __init__(self):
        self.client = None
        self.project_id = config.PROJECT_ID
        self.dataset_id = config.DATASET_ID
        self.credentials_path = config.CREDENTIALS_PATH

    def initialize_client(self):
        """Initialize BigQuery client"""
        try:
            self.client = bigquery.Client.from_service_account_json(
                self.credentials_path, project=self.project_id
            )
            logger.info(f"SUCCESS: Connected to BigQuery project: {self.project_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            return False

    def fetch_historical_data(self, days_back=60):
        """
        Fetch historical price data for model training/fine-tuning

        Args:
            days_back (int): Number of days of historical data to fetch

        Returns:
            pd.DataFrame: DataFrame with columns [product_id, date, price]
        """
        try:
            logger.info(
                f"Fetching {days_back} days of historical data from BigQuery..."
            )

            query = f"""
            SELECT
                v.variant_id,
                dd.full_date as date,
                fpp.current_price as price
            FROM `{self.project_id}.{self.dataset_id}.{config.TABLE_FACT_PRODUCT_PRICE}` fpp
            JOIN `{self.project_id}.{self.dataset_id}.DimDate` dd
                ON fpp.date_id = dd.date_id
            JOIN `{self.project_id}.{self.dataset_id}.{config.TABLE_DIM_VARIANT}` v
                ON fpp.variant_id = v.variant_id
            WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
                AND fpp.is_available = TRUE
                AND fpp.current_price > 0
            ORDER BY v.variant_id, dd.full_date
            """

            df = self.client.query(query).to_dataframe()

            if len(df) == 0:
                logger.error("No data returned from BigQuery")
                return None

            # Convert date to datetime and sort
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values(["variant_id", "date"])

            # Rename variant_id to product_id for consistency
            df = df.rename(columns={"variant_id": "product_id"})

            logger.info(
                f"SUCCESS: Fetched {len(df)} records for {df['product_id'].nunique()} unique variants"
            )
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")

            return df

        except Exception as e:
            logger.error(f"Failed to fetch historical data: {e}")
            return None

    def fetch_available_historical_data(self):
        """
        Fetch all available historical price data for model training/fine-tuning

        Returns:
            pd.DataFrame: DataFrame with columns [product_id, date, price]
        """
        try:
            logger.info("Fetching all available historical data from BigQuery...")

            query = f"""
            SELECT
                v.variant_id,
                dd.full_date as date,
                fpp.current_price as price
            FROM `{self.project_id}.{self.dataset_id}.{config.TABLE_FACT_PRODUCT_PRICE}` fpp
            JOIN `{self.project_id}.{self.dataset_id}.DimDate` dd
                ON fpp.date_id = dd.date_id
            JOIN `{self.project_id}.{self.dataset_id}.{config.TABLE_DIM_VARIANT}` v
                ON fpp.variant_id = v.variant_id
            WHERE fpp.is_available = TRUE
                AND fpp.current_price > 0
            ORDER BY v.variant_id, dd.full_date
            """

            df = self.client.query(query).to_dataframe()

            if len(df) == 0:
                logger.error("No data returned from BigQuery")
                return None

            # Convert date to datetime and sort
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values(["variant_id", "date"])

            # Rename variant_id to product_id for consistency
            df = df.rename(columns={"variant_id": "product_id"})

            logger.info(
                f"SUCCESS: Fetched {len(df)} records for {df['product_id'].nunique()} unique variants"
            )
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")

            return df

        except Exception as e:
            logger.error(f"Failed to fetch available historical data: {e}")
            return None

    def ensure_model_record(self, model_id=None):
        """
        Ensure model record exists in DimModel table

        Args:
            model_id (int): Model ID to check/create
        """
        if model_id is None:
            model_id = config.MODEL_ID

        try:
            # Check if model record exists
            check_query = f"""
            SELECT COUNT(*) as count
            FROM `{self.project_id}.{self.dataset_id}.{config.TABLE_DIM_MODEL}`
            WHERE model_id = {model_id}
            """

            result = list(self.client.query(check_query).result())[0]

            if result.count == 0:
                # Insert model record
                insert_query = f"""
                INSERT INTO `{self.project_id}.{self.dataset_id}.{config.TABLE_DIM_MODEL}`
                (model_id, model_name, model_version, model_type, training_date, 
                 performance_metrics_json, hyperparameters_json)
                VALUES (
                    {model_id},
                    'LSTM_Price_Forecaster',
                    'v1.0_finetuned',
                    'LSTM_Seq2Seq',
                    CURRENT_DATE(),
                    '{{}}',
                    '{{"hidden_size": {config.HIDDEN_SIZE}, "num_layers": {config.NUM_LAYERS}, "sequence_length": {config.SEQUENCE_LENGTH}}}'
                )
                """

                self.client.query(insert_query).result()
                logger.info(f"SUCCESS: Created model record with ID {model_id}")
            else:
                logger.info(f"SUCCESS: Model record {model_id} already exists")

        except Exception as e:
            logger.error(f"Failed to ensure model record: {e}")

    def upload_predictions_concurrent(
        self, predictions, model_id=None, batch_size=5000, max_workers=4
    ):
        """
        Upload predictions to FactPriceForecast table using concurrent batch processing

        Args:
            predictions (list): List of prediction dictionaries
            model_id (int): Model ID for the predictions
            batch_size (int): Number of predictions per batch
            max_workers (int): Maximum number of concurrent upload threads

        Returns:
            bool: Success status
        """
        if model_id is None:
            model_id = config.MODEL_ID

        try:
            if not predictions:
                logger.error("No predictions to upload")
                return False

            logger.info(
                f"Uploading {len(predictions)} predictions to BigQuery using concurrent batches..."
            )
            logger.info(f"Batch size: {batch_size}, Max workers: {max_workers}")

            # Ensure model record exists
            self.ensure_model_record(model_id)

            # Delete existing predictions for this model from today
            table_id = f"{self.project_id}.{self.dataset_id}.{config.TABLE_FACT_PRICE_FORECAST}"

            delete_query = f"""
            DELETE FROM `{table_id}`
            WHERE DATE(created_at) = CURRENT_DATE()
                AND model_id = {model_id}
            """

            self.client.query(delete_query).result()
            logger.info("SUCCESS: Cleared existing predictions for today")

            # Convert predictions to DataFrame for efficient batch upload
            import pandas as pd
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import time

            df_data = []
            for idx, pred in enumerate(predictions):
                # Ensure proper data types for BigQuery compatibility
                forecast_date = pred["forecast_date"]
                if hasattr(forecast_date, "strftime"):
                    forecast_date = forecast_date.strftime("%Y-%m-%d")
                elif isinstance(forecast_date, str):
                    forecast_date = forecast_date
                else:
                    forecast_date = str(forecast_date)

                created_at = pred["created_at"]
                if hasattr(created_at, "strftime"):
                    created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")
                elif isinstance(created_at, str):
                    created_at = created_at
                else:
                    created_at = str(created_at)

                df_data.append(
                    {
                        "forecast_id": idx
                        + 1,  # Sequential forecast_id starting from 1
                        "variant_id": int(pred["variant_id"]),
                        "model_id": int(pred["model_id"]),
                        "forecast_date": forecast_date,
                        "predicted_price": float(pred["predicted_price"]),
                        "confidence_upper": float(pred["confidence_upper"]),
                        "confidence_lower": float(pred["confidence_lower"]),
                        "created_at": created_at,
                    }
                )

            df = pd.DataFrame(df_data)

            # Explicitly set data types to avoid PyArrow conversion issues
            df["forecast_id"] = df["forecast_id"].astype("int64")
            df["variant_id"] = df["variant_id"].astype("int64")
            df["model_id"] = df["model_id"].astype("int64")
            df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date
            df["predicted_price"] = df["predicted_price"].astype("float64")
            df["confidence_upper"] = df["confidence_upper"].astype("float64")
            df["confidence_lower"] = df["confidence_lower"].astype("float64")
            df["created_at"] = pd.to_datetime(df["created_at"])

            # Get table reference
            table_ref = self.client.get_table(table_id)

            # Configure job with explicit schema to avoid type inference issues
            from google.cloud.bigquery import SchemaField

            schema = [
                SchemaField("forecast_id", "INTEGER", mode="REQUIRED"),
                SchemaField("variant_id", "INTEGER", mode="REQUIRED"),
                SchemaField("model_id", "INTEGER", mode="REQUIRED"),
                SchemaField("forecast_date", "DATE", mode="REQUIRED"),
                SchemaField("predicted_price", "FLOAT", mode="NULLABLE"),
                SchemaField("confidence_upper", "FLOAT", mode="NULLABLE"),
                SchemaField("confidence_lower", "FLOAT", mode="NULLABLE"),
                SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            ]

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_NEVER",
                schema=schema,
                autodetect=False,  # Use explicit schema instead of autodetect
            )

            # Split into batches
            batches = []
            for i in range(0, len(df), batch_size):
                batch_df = df[i : i + batch_size]
                batches.append((batch_df, i // batch_size + 1))

            total_batches = len(batches)
            logger.info(f"Created {total_batches} batches for concurrent upload")

            def upload_batch(batch_data):
                """Upload a single batch using SQL INSERT for better compatibility"""
                batch_df, batch_num = batch_data
                try:
                    # Create a new client for this thread
                    thread_client = bigquery.Client.from_service_account_json(
                        self.credentials_path, project=self.project_id
                    )

                    # Build SQL INSERT statement for the batch
                    insert_values = []
                    for _, row in batch_df.iterrows():
                        insert_values.append(
                            f"({row['forecast_id']}, {row['variant_id']}, {row['model_id']}, '{row['forecast_date']}', "
                            f"{row['predicted_price']}, {row['confidence_upper']}, {row['confidence_lower']}, "
                            f"TIMESTAMP('{row['created_at']}'))"
                        )

                    # Execute batch INSERT
                    insert_query = f"""
                    INSERT INTO `{table_id}` 
                    (forecast_id, variant_id, model_id, forecast_date, predicted_price, confidence_upper, confidence_lower, created_at)
                    VALUES {','.join(insert_values)}
                    """

                    job = thread_client.query(insert_query)
                    job.result()  # Wait for job to complete

                    return len(batch_df), batch_num, None
                except Exception as e:
                    return 0, batch_num, str(e)

            # Upload batches concurrently
            uploaded_count = 0
            failed_batches = 0
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all batch upload tasks
                future_to_batch = {
                    executor.submit(upload_batch, batch): batch for batch in batches
                }

                # Process completed uploads
                for future in as_completed(future_to_batch):
                    batch_size_actual, batch_num, error = future.result()

                    if error:
                        logger.error(f"Batch {batch_num} failed: {error}")
                        failed_batches += 1
                    else:
                        uploaded_count += batch_size_actual
                        logger.info(
                            f"Batch {batch_num}/{total_batches}: Uploaded {batch_size_actual} predictions ({uploaded_count}/{len(predictions)} total)"
                        )

            elapsed_time = time.time() - start_time
            rate = uploaded_count / elapsed_time if elapsed_time > 0 else 0

            logger.info(
                f"SUCCESS: Uploaded {uploaded_count}/{len(predictions)} predictions in {elapsed_time:.2f}s ({rate:.0f} predictions/sec)"
            )
            if failed_batches > 0:
                logger.warning(f"Failed batches: {failed_batches}/{total_batches}")

            return uploaded_count > 0

        except Exception as e:
            logger.error(f"Failed to upload predictions to BigQuery: {e}")
            return False

    def upload_predictions_batch(self, predictions, model_id=None, batch_size=10000):
        """
        Upload predictions to FactPriceForecast table using optimized batching (fallback method)
        """
        return self.upload_predictions_concurrent(
            predictions, model_id, batch_size, max_workers=1
        )

    def upload_predictions_simple_batch(
        self, predictions, model_id=None, batch_size=1000
    ):
        """
        Simple batch upload without concurrency for reliability
        """
        if model_id is None:
            model_id = config.MODEL_ID

        try:
            if not predictions:
                logger.error("No predictions to upload")
                return False

            logger.info(
                f"Uploading {len(predictions)} predictions using simple batching (batch size: {batch_size})..."
            )

            # Ensure model record exists
            self.ensure_model_record(model_id)

            # Delete existing predictions for this model from today
            table_id = f"{self.project_id}.{self.dataset_id}.{config.TABLE_FACT_PRICE_FORECAST}"

            delete_query = f"""
            DELETE FROM `{table_id}`
            WHERE DATE(created_at) = CURRENT_DATE()
                AND model_id = {model_id}
            """

            self.client.query(delete_query).result()
            logger.info("SUCCESS: Cleared existing predictions for today")

            # Process in batches
            total_batches = (len(predictions) + batch_size - 1) // batch_size
            uploaded_count = 0

            import time

            start_time = time.time()

            for i in range(0, len(predictions), batch_size):
                batch = predictions[i : i + batch_size]
                batch_num = (i // batch_size) + 1

                try:
                    # Build SQL INSERT for this batch
                    insert_values = []
                    for idx, pred in enumerate(batch):
                        # Generate forecast_id (using batch index and prediction index)
                        forecast_id = uploaded_count + idx + 1

                        # Format dates properly
                        forecast_date = pred["forecast_date"]
                        if hasattr(forecast_date, "strftime"):
                            forecast_date = forecast_date.strftime("%Y-%m-%d")

                        created_at = pred["created_at"]
                        if hasattr(created_at, "strftime"):
                            created_at = created_at.strftime("%Y-%m-%d %H:%M:%S")

                        insert_values.append(
                            f"({forecast_id}, {pred['variant_id']}, {pred['model_id']}, '{forecast_date}', "
                            f"{pred['predicted_price']}, {pred['confidence_upper']}, {pred['confidence_lower']}, "
                            f"TIMESTAMP('{created_at}'))"
                        )

                    # Execute batch INSERT
                    insert_query = f"""
                    INSERT INTO `{table_id}` 
                    (forecast_id, variant_id, model_id, forecast_date, predicted_price, confidence_upper, confidence_lower, created_at)
                    VALUES {','.join(insert_values)}
                    """

                    job = self.client.query(insert_query)
                    job.result()

                    uploaded_count += len(batch)
                    elapsed = time.time() - start_time
                    rate = uploaded_count / elapsed if elapsed > 0 else 0

                    logger.info(
                        f"Batch {batch_num}/{total_batches}: Uploaded {len(batch)} predictions ({uploaded_count}/{len(predictions)} total) - {rate:.0f}/sec"
                    )

                except Exception as batch_error:
                    logger.error(f"Failed to upload batch {batch_num}: {batch_error}")
                    continue

            total_time = time.time() - start_time
            final_rate = uploaded_count / total_time if total_time > 0 else 0

            logger.info(
                f"SUCCESS: Uploaded {uploaded_count}/{len(predictions)} predictions in {total_time:.2f}s ({final_rate:.0f} predictions/sec)"
            )
            return uploaded_count > 0

        except Exception as e:
            logger.error(f"Failed to upload predictions: {e}")
            return False

    def upload_predictions(self, predictions, model_id=None):
        """
        Upload predictions using simple reliable batch method
        """
        # Try concurrent first, fallback to simple batch
        try:
            logger.info("Attempting concurrent upload...")
            success = self.upload_predictions_concurrent(
                predictions, model_id, batch_size=2000, max_workers=2
            )
            if success:
                return True
        except Exception as e:
            logger.warning(f"Concurrent upload failed: {e}")

        logger.info("Falling back to simple batch upload...")
        return self.upload_predictions_simple_batch(
            predictions, model_id, batch_size=1000
        )
