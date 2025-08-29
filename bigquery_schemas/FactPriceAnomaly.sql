-- Table: FactPriceAnomaly
-- Stores the output of the Anomaly Detection ML model for product price anomalies.

CREATE TABLE IF NOT EXISTS `price-pulse-470211.warehouse.FactPriceAnomaly` (
  anomaly_id INT64 NOT NULL,                      -- Surrogate key for the anomaly record
  price_fact_id INT64 NOT NULL,                   -- References FactProductPrice.price_fact_id (not enforced)
  model_id INT64 NOT NULL,                        -- References DimModel.model_id (not enforced)
  anomaly_score FLOAT64,                          -- Score indicating degree of anomaly
  anomaly_type STRING,                            -- Type of anomaly (e.g., 'PRICE_DROP')
  status STRING NOT NULL,                         -- Review status ('PENDING_REVIEW', 'CONFIRMED_SALE', etc.)
  reviewed_by_user_id INT64,                      -- User ID who reviewed the anomaly (external reference)
  created_at TIMESTAMP                            -- Timestamp when the anomaly was detected
)
OPTIONS(
  description="Fact table for ML-detected price anomalies, including review status and model reference."
);

-- Note: BigQuery does not enforce primary or foreign key constraints.
-- price_fact_id, model_id, and reviewed_by_user_id should reference their respective tables, but this is not enforced.
-- Note: BigQuery does not support DEFAULT values in schema definitions.
-- Set default values (e.g., status='PENDING_REVIEW', created_at=CURRENT_TIMESTAMP) in your ETL/data pipeline when inserting data.